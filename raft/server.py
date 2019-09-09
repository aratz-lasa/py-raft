import asyncio

from .state_machine import RaftStateMachine, State

ELECTION_TIMEOUT = 0.5


class ServerPersistentStateData:
    def __init__(self):
        current_term = None
        voted_for = None
        log = None


class ServerVolatileStateData:
    commit_index = None
    last_applied = None


class LeaderVolatileStateData:
    next_index = None
    match_index = None


class Server:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port


class RaftServer(Server, RaftStateMachine):
    def __init__(self, cluster, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.cluster = cluster or []
        self._leader_hbeat = asyncio.Event()
        self._server_persistent_state_data = ServerPersistentStateData()
        self._server_volatile_state_data = ServerVolatileStateData()
        self._leader_volatile_state_data = None
        self._running_tasks = []

    async def join_cluster(self, random_server):
        pass
        # TODO: generate new configuration with itself and join cluster

    async def _start_tasks(self):
        self._running_tasks = [await asyncio.create_task(self._elections_task())]

    async def _elections_task(self):
        while True:
            try:
                await asyncio.wait_for(self._leader_hbeat.wait(), timeout=ELECTION_TIMEOUT)
            except asyncio.TimeoutError:
                await self._change_state(State.CANDIDATE)
            finally:
                self._leader_hbeat.clear()
