import json
import math
from dataclasses import dataclass, field
from enum import IntEnum, auto
from typing import Any, List, Union, Dict

from .. import errors
from .. import state_machine


# Structures
@dataclass
class RaftMessage:
    opcode: Any = field(default=None)
    payload: Any = field(default=None)


# CONSTANTS
SEPARATOR = b","
DOUBLE_SEPARATOR = SEPARATOR * 2
OTHER_SEPARATOR = b"#"
OTHER_SUB_SEPARATOR = b"~"
OK_RESPONSE = 0
EMPTY = b""


class RPC(IntEnum):
    # Opcodes
    APPEND_ENTRY = auto()
    COMMAND_REQUEST = auto()
    REQUEST_VOTE = auto()
    ADD_CLUSTER_CONFIGURATION = auto()
    GET_CLUSTER_CONFIGURATION = auto()
    # Ok
    OK = auto()
    # Errors
    ERROR_TERM = auto()
    ERROR_ENTRY = auto()


## Translation functions
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


def encode_request_vote(
    term: int, candidate_id: bytes, last_log_index: int, last_log_term: int
):
    json_payload = {
        "term": term,
        "candidate_id": candidate_id,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
    }
    return json.dumps(json_payload).encode()


def decode_request_vote(payload: bytes):
    json_payload = json.loads(payload.decode())
    term = json_payload["term"]
    candidate_id = json_payload["candidate_id"]
    last_log_index = json_payload["last_log_index"]
    last_log_term = json_payload["last_log_term"]
    return term, candidate_id, last_log_index, last_log_term


def encode_append_entry(
    term: int,
    leader_id: bytes,
    prev_log_index: int,
    prev_log_term: int,
    entries: List[state_machine.Entry],
    leader_commit: int,
):
    json_payload = {
        "term": term,
        "leader_id": leader_id,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries,
        "leader_commit": leader_commit,
    }
    return json.dumps(json_payload, default=raft_json_encoder).encode()


def decode_append_entry(payload: bytes):
    json_payload = json.loads(payload.decode(), object_hook=raft_json_decoder)
    term = json_payload["term"]
    leader_id = json_payload["leader_id"]
    prev_log_index = (json_payload["prev_log_index"],)
    prev_log_term = json_payload["prev_log_term"]
    entries = json_payload["entries"]
    leader_commit = json_payload["leader_commit"]
    return term, leader_id, prev_log_index, prev_log_term, entries, leader_commit


def encode_command_request(command: state_machine.Command):
    pass  # TODO


def decode_command_request(payload: bytes):
    pass  # TODO


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


## UTILS


def dump_data_with_length(data: bytes):
    data_length = len(data)
    return (
        data_length.to_bytes(get_int_bytes_amount(data_length), "big")
        + SEPARATOR
        + data
    )


def get_int_bytes_amount(number: int):
    if number == 0:
        return 1
    return math.ceil(math.log(number, 256))


def raft_json_encoder(obj: Any) -> Union[Dict, List]:
    if hasattr(obj, "as_dict"):
        return obj.as_dict
    else:
        type_name = obj.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def raft_json_decoder(raw_json: Union[Dict, List]) -> Any:
    if "__command__" in raw_json:
        del raw_json["__command__"]
        return state_machine.Command(**raw_json)
    elif "__entry__" in raw_json:
        del raw_json["__entry__"]
        return state_machine.Entry(**raw_json)
    return raw_json
