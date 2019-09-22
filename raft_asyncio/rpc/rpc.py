import asyncio
from contextlib import asynccontextmanager
from typing import Tuple

from raft_asyncio import server, errors
from .protocol import *

CONNECTION_TIMEOUT = 5


# UTILS
def check_consistency(
    raft_server: server.RaftServer, term: int, prev_log: Tuple[int, int] = None
):
    if term < raft_server._current_term:
        raft_server._change_state(state_machine.State.FOLLOWER)
        raise errors.TermConsistencyError(term)
    if prev_log:
        prev_log_index, prev_log_term = prev_log
        try:
            if raft_server._log[prev_log_index] != prev_log_term:
                raise errors.EntriesConsistencyError()
        except:
            raise errors.EntriesConsistencyError()


def consistency_decorator(func):
    async def wrapper(raft_server: server.RaftServer, writer, *args, **kwargs):
        try:
            await func(raft_server, writer, *args, **kwargs)
        except errors.TermConsistencyError:
            payload = {"term": raft_server._current_term}  # TODO: more data?
            payload = json.dumps(payload).encode()
            await encode_send_msg(writer, RPC.ERROR_TERM, payload)
        except errors.EntriesConsistencyError:
            payload = EMPTY  # TODO: send data?
            await encode_send_msg(writer, RPC.ERROR_ENTRY, payload)

    return wrapper


async def send_ok(writer):
    await encode_send_msg(writer, RPC.OK, EMPTY)


async def read_check_ok(reader):
    message = await read_decode_msg(reader)
    if message.opcode is RPC.ERROR_TERM:
        term = message.payload["term"]
        raise errors.TermConsistencyError(term)
    elif message.opcode is RPC.ERROR_ENTRY:
        raise errors.EntriesConsistencyError()
    elif message.opcode is not RPC.OK:
        raise errors.RPCError()


async def read_decode_msg(reader):
    opcode = (await reader.readuntil(SEPARATOR))[:-1]
    opcode = int.from_bytes(opcode, "big")
    payload_length = (await reader.readuntil(SEPARATOR))[:-1]
    payload_length = int.from_bytes(payload_length, "big")
    payload = await reader.read(payload_length)
    return RaftMessage(opcode, payload)


async def encode_send_msg(writer, opcode: int, payload: bytes):
    msg = bytes([opcode]) + SEPARATOR + dump_data_with_length(payload)
    writer.write(msg)
    await writer.drain()


## Outgoing calls
class RemoteRaftServer(server.ClusterMember):
    async def append_entries(
        self,
        term: int,
        leader_id: bytes,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[state_machine.Entry],
        leader_commit: int,
    ):
        """
        Invoked by leader to replicate log entries; also used as heartbeat
        :param term:            leader’s term
        :param leader_id:       so follower can redirect clients
        :param prev_log_index:  index of log entry immediately precedingnew ones
        :param prev_log_term:   term of prevLogIndex entry
        :param entries:         log entry to store (empty for heartbeat)
        :param leader_commit:   leader’s commitIndex
        :return:
                term:       currentTerm, for leader to update itself
                success:    true if follower contained entry matching prevLogIndex and prevLogTerm
        """
        with connect(self) as connection:
            reader, writer = connection
            payload = encode_append_entry(
                term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
            )
            await encode_send_msg(writer, RPC.APPEND_ENTRY, payload)
            await read_check_ok(reader)  # TODO?

    async def request_vote(
        self, term: int, candidate_id: bytes, last_log_index: int, last_log_term: int
    ):
        """
        Invoked by candidates to gather votes
        :param term:            candidate’s term
        :param candidate_id:    candidate requesting vote
        :param last_log_index:  index of candidate’s last log entry
        :param last_log_term:   term of candidate’s last log entry
        :return:
                term:           current_term, for candidate to update itself
                vote_granted:   true means candidate received vote
        """
        with connect(self) as connection:
            reader, writer = connection
            payload = encode_request_vote(
                term, candidate_id, last_log_index, last_log_term
            )
            await encode_send_msg(writer, RPC.REQUEST_VOTE, payload)
            vote = await reader.read(1)
        return vote

    async def command_request(self, command: state_machine.Command):
        with connect(self) as connection:
            reader, writer = connection
            payload = encode_command_request(command)
            await encode_send_msg(writer, RPC.COMMAND_REQUEST, payload)
            # TODO

    async def add_cluster_configuration(self, cluster: List[server.ClusterMember]):
        pass  # TODO

    async def get_cluster_configuration(self):
        with connect(self) as connection:
            reader, writer = connection
            payload = EMPTY
            await encode_send_msg(writer, RPC.GET_CLUSTER_CONFIGURATION, payload)

            # TODO: read and return cluster and leader


@asynccontextmanager
async def connect(dst: RemoteRaftServer, raise_error=True):
    conn = asyncio.open_connection(dst.ip, dst.port)
    try:
        reader, writer = await asyncio.wait_for(conn, timeout=CONNECTION_TIMEOUT)
        yield (reader, writer)
    except (asyncio.TimeoutError, ConnectionError):
        pass
    finally:
        if "writer" in locals():
            writer.close()


## Ingoing calls
# Main switch case for Requests
async def handle_request(
    raft_server: server.RaftServer, connection: Tuple, message: RaftMessage
):
    reader, writer = connection

    if message.opcode is RPC.REQUEST_VOTE:
        raft_server._leader_hbeat.set()
        await process_vote(raft_server, writer, message)
    elif message.opcode is RPC.APPEND_ENTRY:
        raft_server._leader_hbeat.set()
        await process_entry(raft_server, writer, message)
    elif message.opcode is RPC.COMMAND_REQUEST:
        if raft_server._im_leader():
            await process_command_request(raft_server, writer, message)
        else:
            await encode_send_msg(writer, RPC.ERROR_LEADER, raft_server.leader.id)
    elif message.opcode is RPC.ADD_CLUSTER_CONFIGURATION:
        pass  # TODO
    elif message.opcode is RPC.GET_CLUSTER_CONFIGURATION:
        await process_cluster(raft_server, writer)


@consistency_decorator
async def process_vote(
    raft_server: server.RaftServer, writer, raft_message: RaftMessage
):
    term, id, last_log_index, last_log_term = decode_request_vote(raft_message.payload)
    check_consistency(raft_server, term)
    vote = is_vote_granted(raft_server, term, id, last_log_index, last_log_term)
    writer.write(int(vote))
    await writer.drain()


def is_vote_granted(raft_server, term, candidate_id, last_log_index, last_log_term):
    if raft_server._current_term == term and raft_server._voted_for != candidate_id:
        return False
    elif raft_server._log and last_log_term < raft_server._log[-1].term:
        return False
    elif (
        raft_server._log
        and last_log_term == raft_server._log[-1].term
        and last_log_index < raft_server._last_applied
    ):
        return False
    return True


@consistency_decorator
async def process_entry(
    raft_server: server.RaftServer, writer, raft_message: RaftMessage
):
    term, leader_id, prev_log_index, prev_log_term, entries, leader_commit = decode_append_entry(
        raft_message.payload
    )
    check_consistency(raft_server, term, entries)
    if raft_server._im_leader() and term > raft_server._current_term:
        raft_server._change_state(state_machine.State.FOLLOWER)
    if entries:  # It is not a hbeat
        entries_amount = len(entries)
        raft_server._log[-entries_amount:] = entries
        raft_server._commit_index = leader_commit
        raft_server._last_applied = max(
            raft_server._last_applied, raft_server._commit_index
        )
    else:  # It is a hbeat
        raft_server._leader_hbeat.set()


def is_entry_appendable(
    raft_server: server.RaftServer, term: int, prev_log_index: int, prev_log_term: int
):
    if (
        len(raft_server._log) < prev_log_index - 1
        or raft_server._log[prev_log_index] != prev_log_term
    ):
        return False
    return True


@consistency_decorator
async def process_command_request(
    raft_server: server.RaftServer, writer, raft_message: RaftMessage
):
    command = decode_command_request(raft_message.payload)
    await raft_server._queue_command(command)
    await send_ok(writer)


async def process_cluster(raft_server: server.RaftServer, writer):
    pass  # TODO:
