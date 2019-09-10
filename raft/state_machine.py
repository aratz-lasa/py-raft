from collections import namedtuple
from dataclasses import dataclass
from enum import Enum

Command = namedtuple("Command", "key value")


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
        self._commit_index = 0  # index of highest log entry known to becommitted
        self._last_applied = 0  # index of highest log entry applied to statemachine

    async def _change_state(self, new_state: State):
        # TODO
        pass
