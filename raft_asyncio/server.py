import asyncio
from dataclasses import dataclass, field
from typing import Any

from . import rpc, utils
from .abc import IRaftServer
from .state_machine import RaftStateMachine, State

ELECTION_TIMEOUT = 0.5


@dataclass
class ServerPersistentStateData:
    log: Any = field(default=None)


@dataclass
class ServerVolatileStateData:
    commit_index: Any = field(default=None)
    last_applied: Any = field(default=None)


@dataclass
class LeaderVolatileStateData:
    next_index: Any = field(default=None)
    match_index: Any = field(default=None)


class Server:
    def __init__(self, ip: str, port: int, cluster):
        self.ip = ip
        self.port = port
        self.id = utils.get_id(self, ip, port)
        self.cluster = cluster or []
        self._leader_hbeat = asyncio.Event()
        self._persistent_state_data = ServerPersistentStateData()
        self._volatile_state_data = ServerVolatileStateData()
        self._leader_volatile_state_data = None
        self._listener_task = None

    async def _start_listening(self):
        self._listener_task = await asyncio.start_server(
            self._handle_request, self.ip, self.port
        )

    async def _handle_request(self, reader, writer):
        opcode = (await reader.readuntil(rpc.SEPARATOR))[:-1]
        opcode = int.from_bytes(opcode, "big")
        payload_length = (await reader.readuntil(rpc.SEPARATOR))[:-1]
        payload_length = int.from_bytes(payload_length, "big")
        payload = await reader.read(payload_length)
        request = rpc.Request(reader, writer, opcode, payload)
        await rpc.handle_request(self, request)
        writer.close()


class RaftServer(IRaftServer, Server, RaftStateMachine):
    def __init__(
        self, ip: str, port: int, cluster, state: State = State.FOLLOWER, log=None
    ):
        Server.__init__(self, ip, port, cluster)
        RaftStateMachine.__init__(self, state, log)

        self._current_term = 0  # latest term server has see
        self._voted_for = None  # candidateId that received vote in currentterm
        self._next_indexes = (
            {}
        )  # for each server, index of the next log entryto send to that server
        self._match_indexes = (
            {}
        )  # for each server, index of highest log entryknown to be replicated on server
        # TODO: init tasks

    async def update_state(self, key, value):
        pass
        # TODO

    async def join_cluster(self, random_server):
        pass
        # TODO: generate new configuration with itself and join cluster

    async def leave_cluster(self):
        pass
        # TODO: generate new configuration without itself and leave cluster

    async def _elections_task(self):
        while True:
            try:
                await asyncio.wait_for(
                    self._leader_hbeat.wait(), timeout=ELECTION_TIMEOUT
                )  # TODO: random timeout
            except asyncio.TimeoutError:
                await self._begin_election()
            finally:
                self._leader_hbeat.clear()

    async def _begin_election(self):
        self._current_term += 1
        await self._change_state(State.CANDIDATE)
        last_log_index = self._last_applied
        last_log_term = 0 if not self._log else self._log[last_log_index].term
        servers = [s for s in self.cluster if s is not self]
        voting_rpcs = list(
            map(
                lambda s: s.request_vote(
                    self._current_term, self.id, last_log_index, last_log_term
                ),
                servers,
            )
        )
        granted_votes = 1  # 1 -> its own vote
        votes = 1
        election_win = False
        for next_vote in asyncio.as_completed(*voting_rpcs):
            try:
                vote = (await next_vote).result()
                granted_votes += int(vote)
            except:
                pass
            votes += 1
            if granted_votes > len(self.cluster) // 2:
                election_win = True
        if election_win:
            pass
            # TODO: change to leader
