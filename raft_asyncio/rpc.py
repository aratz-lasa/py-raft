import asyncio
import math
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import IntEnum, auto
from typing import Any, List

from . import server, utils, state_machine, errors

# CONSTANTS
SEPARATOR = b","
OK_RESPONSE = 0
CONNECTION_TIMEOUT = 5


# RPC opcodes
class RPC(IntEnum):
    APPEND_ENTRY = auto()
    COMMIT_COMMAND = auto()
    REQUEST_VOTE = auto()


## Outgoing calls
class RemoteRaftServer:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id = utils.get_id(self, ip, port)

    async def append_entry(
        self,
        term: int,
        leader_id: bytes,
        prev_log_index: int,
        prev_log_term: int,
        entry: state_machine.Command,
        leader_commit: int,
    ):
        """
        Invoked by leader to replicate log entries; also used as heartbeat
        :param term:            leader’s term
        :param leader_id:       so follower can redirect clients
        :param prev_log_index:  index of log entry immediately precedingnew ones
        :param prev_log_term:   term of prevLogIndex entry
        :param entry:         log entry to store (empty for heartbeat)
        :param leader_commit:   leader’s commitIndex
        :return:
                term:       currentTerm, for leader to update itself
                success:    true if follower contained entry matching prevLogIndex and prevLogTerm
        """
        with connect(self) as connection:
            [reader, writer] = connection
            payload = SEPARATOR.join(
                [
                    term.to_bytes(_get_int_bytes_amount(term), "big"),
                    leader_id,
                    prev_log_index.to_bytes(
                        _get_int_bytes_amount(prev_log_index), "big"
                    ),
                    prev_log_term.to_bytes(_get_int_bytes_amount(prev_log_term), "big"),
                    entry.key,
                    entry.value,
                    leader_commit.to_bytes(_get_int_bytes_amount(leader_commit), "big"),
                ]
            )
            writer.write(_dump_request(RPC.APPEND_ENTRY, payload))
            await writer.drain()
            await _check_ok(reader)

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
            [reader, writer] = connection
            payload = SEPARATOR.join(
                [
                    term.to_bytes(_get_int_bytes_amount(term), "big"),
                    candidate_id,
                    last_log_index.to_bytes(
                        _get_int_bytes_amount(last_log_index), "big"
                    ),
                    last_log_term.to_bytes(_get_int_bytes_amount(last_log_term), "big"),
                ]
            )
            writer.write(_dump_request(RPC.REQUEST_VOTE, payload))
            await writer.drain()
            vote = await reader.read()
        return vote

    async def commit_command(self, command: state_machine.Command):
        with connect(self) as connection:
            [reader, writer] = connection
            payload = SEPARATOR.join([command.key, command.value])
            writer.write(_dump_request(RPC.COMMIT_COMMAND, payload))
            await writer.drain()
            await _check_ok(reader)


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


def _dump_request(opcode: int, payload: bytes) -> bytes:
    return bytes([opcode]) + SEPARATOR + _dump_data_with_length(payload)


def _dump_data_with_length(data: bytes):
    data_length = len(data)
    return (
        data_length.to_bytes(_get_int_bytes_amount(data_length), "big")
        + SEPARATOR
        + data
    )


def _get_int_bytes_amount(number: int):
    if number == 0:
        return 1
    return math.ceil(math.log(number, 256))


async def _send_ok(writer):
    writer.write(bytes([OK_RESPONSE]))
    await writer.drain()


async def _check_ok(reader):
    ok = int.from_bytes(await reader.read(1), "big") == OK_RESPONSE
    if not ok:
        raise errors.RPCError()


## Ingoing calls


@dataclass
class Request:
    reader: Any = field(default=None)
    writer: Any = field(default=None)
    opcode: Any = field(default=None)
    payload: Any = field(default=None)


async def handle_request(raft_server: server.RaftServer, request: Request):
    reader = request.reader
    writer = request.writer

    if request.opcode is RPC.REQUEST_VOTE:
        payload_lst = request.payload.split(SEPARATOR, 3)
        term = int.from_bytes(payload_lst[0], "big")
        id = payload_lst[1]
        last_log_index = int.from_bytes(payload_lst[2], "big")
        last_log_term = int.from_bytes(payload_lst[3], "big")

        writer.write(
            int(is_vote_granted(raft_server, term, id, last_log_index, last_log_term))
        )
        await writer.drain()
    elif request.opcode is RPC.APPEND_ENTRY:
        payload_lst = request.payload.split(SEPARATOR, 6)
        term = int.from_bytes(payload_lst[0], "big")
        leader_id = payload_lst[1]
        prev_log_index = int.from_bytes(payload_lst[2], "big")
        prev_log_term = int.from_bytes(payload_lst[3], "big")
        entry = state_machine.Command(payload_lst[4], payload_lst[5])
        leader_commit = int.from_bytes(payload_lst[6], "big")

        if is_entry_appendable(
            raft_server,
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entry,
            leader_commit,
        ):
            raft_server._append_command(entry.command)
            if leader_commit > raft_server._commit_index:
                raft_server._commit_index = min(
                    leader_commit, raft_server._last_applied
                )  # TODO: Check why
            await _send_ok(writer)
        else:
            pass  # TODO

    elif request.opcode is RPC.COMMIT_COMMAND:
        [key, value] = request.payload.split(SEPARATOR, 1)
        command = state_machine.Command(key, value)
        await raft_server._queue_command(command)
        await _send_ok(writer)


def is_vote_granted(raft_server, term, candidate_id, last_log_index, last_log_term):
    granted = True
    if term < raft_server._current_term:
        granted = False
    elif raft_server._current_term == term and raft_server._voted_for != candidate_id:
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


def is_entry_appendable(
    raft_server: server.RaftServer,
    term: int,
    leader_id: bytes,
    prev_log_index: int,
    prev_log_term: int,
    entry: state_machine.Command,
    leader_commit: int,
):
    if term < raft_server._current_term:
        return False
    elif (
        len(raft_server._log) < prev_log_index - 1
        or raft_server._log[prev_log_index] != prev_log_term
    ):
        return False
    return True
