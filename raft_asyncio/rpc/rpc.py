import asyncio
import json
from contextlib import asynccontextmanager
from typing import List, Tuple

from raft_asyncio import server, state_machine, errors
from . import protocol as prot

CONNECTION_TIMEOUT = 5


# UTILS
def check_consistency(
    raft_server: server.RaftServer, term: int, entries: List[state_machine.Entry] = None
):
    if term < raft_server._current_term:
        raft_server._change_state(state_machine.State.FOLLOWER)
        raise errors.TermConsistencyError(term)
    if entries:
        pass  # TODO: check entries consistency


def consistency_decorator(func):
    async def wrapper(raft_server: server.RaftServer, writer, *args, **kwargs):
        try:
            await func(raft_server, writer, *args, **kwargs)
        except errors.TermConsistencyError:
            payload = {"term": raft_server._current_term}  # TODO: more data?
            payload = json.dumps(payload).encode()
            await prot.encode_send_msg(writer, prot.RPC.ERROR_TERM, payload)
        except errors.EntriesConsistencyError:
            payload = prot.EMPTY  # TODO: send data?
            await prot.encode_send_msg(writer, prot.RPC.ERROR_ENTRY, payload)

    return wrapper


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
            payload = prot.encode_append_entry(
                term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
            )
            await prot.encode_send_msg(writer, prot.RPC.APPEND_ENTRY, payload)
            await prot.read_check_ok(reader)  # TODO?

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
            payload = prot.encode_request_vote(
                term, candidate_id, last_log_index, last_log_term
            )
            await prot.encode_send_msg(writer, prot.RPC.REQUEST_VOTE, payload)
            vote = await reader.read(1)
        return vote

    async def command_request(self, command: state_machine.Command):
        with connect(self) as connection:
            reader, writer = connection
            payload = prot.encode_command_request(command)
            await prot.encode_send_msg(writer, prot.RPC.COMMAND_REQUEST, payload)
            # TODO

    async def add_cluster_configuration(self, cluster: List[server.ClusterMember]):
        pass  # TODO

    async def get_cluster_configuration(self):
        with connect(self) as connection:
            reader, writer = connection
            payload = prot.EMPTY
            await prot.encode_send_msg(
                writer, prot.RPC.GET_CLUSTER_CONFIGURATION, payload
            )

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
    raft_server: server.RaftServer, connection: Tuple, message: prot.RaftMessage
):
    reader, writer = connection

    if message.opcode is prot.RPC.REQUEST_VOTE:
        raft_server._leader_hbeat.set()
        await process_vote(raft_server, writer, message)
    elif message.opcode is prot.RPC.APPEND_ENTRY:
        raft_server._leader_hbeat.set()
        await process_entry(raft_server, writer, message)
    elif message.opcode is prot.RPC.COMMAND_REQUEST:
        if raft_server._im_leader():
            await process_command_request(raft_server, writer, message)
        else:
            await prot.encode_send_msg(
                writer, prot.RPC.ERROR_LEADER, raft_server.leader.id
            )
    elif message.opcode is prot.RPC.ADD_CLUSTER_CONFIGURATION:
        pass  # TODO
    elif message.opcode is prot.RPC.GET_CLUSTER_CONFIGURATION:
        await process_cluster(raft_server, writer)


@consistency_decorator
async def process_vote(
    raft_server: server.RaftServer, writer, raft_message: prot.RaftMessage
):
    term, id, last_log_index, last_log_term = prot.decode_request_vote(
        raft_message.payload
    )
    check_consistency(raft_server, term)
    if is_vote_granted(raft_server, term, id, last_log_index, last_log_term):
        writer.write(int(True))
        await writer.drain()
    else:
        pass  # TODO


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
    raft_server: server.RaftServer, writer, raft_message: prot.RaftMessage
):
    term, leader_id, prev_log_index, prev_log_term, entries, leader_commit = prot.decode_append_entry(
        raft_message.payload
    )
    check_consistency(raft_server, term, entries)
    raft_server._leader_hbeat.set()
    if entries:  # It is not a hbeat
        pass  # TODO:
    else:  # It is a hbeat
        raft_server._change_state(state_machine.State.FOLLOWER)
        pass  # TODO:


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
    raft_server: server.RaftServer, writer, raft_message: prot.RaftMessage
):
    command = prot.decode_command_request(raft_message.payload)
    await raft_server._queue_command(command)
    await prot.send_ok(writer)


async def process_cluster(raft_server: server.RaftServer, writer):
    pass  # TODO:
