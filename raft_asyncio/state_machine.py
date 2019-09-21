from dataclasses import dataclass
from enum import Enum
from typing import Union, List, Dict


@dataclass
class Command:
    key: bytes
    value: bytes

    @property
    def as_dict(self) -> Union[List, Dict]:
        return {"__command__": True, "key": self.key, "value": self.value}


@dataclass
class Entry:
    command: Command
    term: int

    @property
    def as_dict(self) -> Union[List, Dict]:
        return {"__entry__": True, "command": self.command, "term": self.term}


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class RaftStateMachine:
    def __init__(self, state: State = State.FOLLOWER, log=None):
        self._state = state
        self._log = log or []
        self._commit_index = 0  # index of highest log entry known to be committed
        self._last_applied = 0  # index of highest log entry applied to statemachine
        self._current_term = 0  # latest term server has see

    def _commit_command(self, command: Command):
        self._commit_index = self._log.index(command)

    def _append_command(self, command: Command):
        entry = Entry(command=command, term=self._current_term)
        self._log.append(entry)
        self._last_applied += 1
