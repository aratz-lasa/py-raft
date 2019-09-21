import asyncio
from contextlib import asynccontextmanager
from typing import List

from raft_asyncio import server, state_machine, errors
from . import protocol as prot

CONNECTION_TIMEOUT = 5


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
            writer.write(prot.encode_msg(prot.RPC.APPEND_ENTRY, payload))
            await writer.drain()
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
            writer.write(prot.encode_msg(prot.RPC.REQUEST_VOTE, payload))
            await writer.drain()
            vote = await reader.read(1)
        return vote

    async def command_request(self, command: state_machine.Command):
        with connect(self) as connection:
            reader, writer = connection
            payload = prot.encode_command_request(command)
            writer.write(prot.encode_msg(prot.RPC.COMMAND_REQUEST, payload))
            await writer.drain()
            # TODO

    async def add_cluster_configuration(self, cluster: List[server.ClusterMember]):
        pass  # TODO

    async def get_cluster_configuration(self):
        with connect(self) as connection:
            reader, writer = connection
            payload = prot.EMPTY
            writer.write(prot.encode_msg(prot.RPC.GET_CLUSTER_CONFIGURATION, payload))
            await writer.drain()

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
async def handle_request(raft_server: server.RaftServer, request: prot.Request):
    reader = request.reader
    writer = request.writer

    if request.opcode is prot.RPC.REQUEST_VOTE:
        raft_server._leader_hbeat.set()
        await process_vote(raft_server, request, writer)
    elif request.opcode is prot.RPC.APPEND_ENTRY:
        raft_server._leader_hbeat.set()
        await process_entry(raft_server, request, writer)
    elif request.opcode is prot.RPC.COMMAND_REQUEST:
        if raft_server._im_leader():
            await process_command_request(raft_server, request, writer)
        else:
            pass  # TODO
    elif request.opcode is prot.RPC.ADD_CLUSTER_CONFIGURATION:
        pass  # TODO
    elif request.opcode is prot.RPC.GET_CLUSTER_CONFIGURATION:
        await process_cluster(raft_server, writer)


async def process_vote(
    raft_server: server.RaftServer, raft_message: prot.RaftMessage, writer
):
    term, id, last_log_index, last_log_term = prot.decode_request_vote(
        raft_message.payload
    )
    if is_vote_granted(raft_server, term, id, last_log_index, last_log_term):
        writer.write(int(True))
        await writer.drain()
    else:
        pass  # TODO


def is_vote_granted(raft_server, term, candidate_id, last_log_index, last_log_term):
    granted = True
    try:
        check_inconsistency(raft_server, term)
    except errors.ConsistencyError:
        granted = False

    if raft_server._current_term == term and raft_server._voted_for != candidate_id:
        granted = False
    elif raft_server._log and last_log_term < raft_server._log[-1].term:
        granted = False
    elif (
        raft_server._log
        and last_log_term == raft_server._log[-1].term
        and last_log_index < raft_server._last_applied
    ):
        granted = False
    return granted


async def process_entry(
    raft_server: server.RaftServer, raft_message: prot.RaftMessage, writer
):
    term, leader_id, prev_log_index, prev_log_term, entries, leader_commit = prot.decode_append_entry(
        raft_message.payload
    )
    if entries:  # It is not a hbeat
        pass  # TODO:
    else:  # It is a hbeat
        pass  # TODO


def is_entry_appendable(
    raft_server: server.RaftServer, term: int, prev_log_index: int, prev_log_term: int
):
    try:
        check_inconsistency(raft_server, term)
    except errors.ConsistencyError:
        return False
    if (
        len(raft_server._log) < prev_log_index - 1
        or raft_server._log[prev_log_index] != prev_log_term
    ):
        return False
    return True


async def process_command_request(
    raft_server: server.RaftServer, raft_message: prot.RaftMessage, writer
):
    command = prot.decode_command_request(raft_message.payload)
    await raft_server._queue_command(command)
    await prot.send_ok(writer)


async def process_cluster(raft_server: server.RaftServer, writer):
    pass  # TODO


# UTILS
async def check_inconsistency(raft_server: server.RaftServer, term):
    if term < raft_server._current_term:
        raft_server._change_state(state_machine.State.FOLLOWER)
        raise errors.ConsistencyError()
