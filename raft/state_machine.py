from enum import Enum


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class RaftStateMachine:
    def __init__(self, state: State = State.FOLLOWER, entries=None):
        self.state = state
        self.entries = entries or {}

    async def _change_state(self, new_state: State):
        # TODO
        pass
