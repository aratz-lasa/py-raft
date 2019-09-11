from dataclasses import dataclass
from enum import Enum


@dataclass
class Command:
    key: bytes
    value: bytes


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


@dataclass
class Entry:
    command: Command
    term: int


class RaftStateMachine:
    def __init__(self, state: State = State.FOLLOWER, log=None):
        self._state = state
        self._log = log or []
        self._commit_index = 0  # index of highest log entry known to be committed
        self._last_applied = 0  # index of highest log entry applied to statemachine
        self._current_term = 0  # latest term server has see

    async def _change_state(self, new_state: State):
        # TODO
        pass

    def _commit_command(self, command: Command):
        pass
        # TODO: after applying it, check as committed

    def _append_command(self, command: Command):
        entry = Entry(command=command, term=self._current_term)
        self._log.append(entry)
        self._last_applied += 1
