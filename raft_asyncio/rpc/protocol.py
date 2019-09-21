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
    reader: Any = field(default=None)
    writer: Any = field(default=None)
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
    APPEND_ENTRY = auto()
    COMMAND_REQUEST = auto()
    REQUEST_VOTE = auto()
    ADD_CLUSTER_CONFIGURATION = auto()
    GET_CLUSTER_CONFIGURATION = auto()


## Translation functions
async def read_decode_msg(reader, writer):
    opcode = (await reader.readuntil(SEPARATOR))[:-1]
    opcode = int.from_bytes(opcode, "big")
    payload_length = (await reader.readuntil(SEPARATOR))[:-1]
    payload_length = int.from_bytes(payload_length, "big")
    payload = await reader.read(payload_length)
    return RaftMessage(reader, writer, opcode, payload)


def encode_msg(opcode: int, payload: bytes) -> bytes:
    return bytes([opcode]) + SEPARATOR + dump_data_with_length(payload)


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
    writer.write(bytes([OK_RESPONSE]))
    await writer.drain()


async def read_check_ok(reader):
    ok = int.from_bytes(await reader.read(1), "big") == OK_RESPONSE
    if not ok:
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
