import asyncio
from typing import List

from . import rpc, utils
from .abc import IRaftServer
from .state_machine import RaftStateMachine, State, Command

ELECTION_TIMEOUT = 0.5
FLEXIBLE_PAXOS_QUORUM = 2 / 6
RPC_TIMEOUT = 1


class Server:
    def __init__(self, ip: str, port: int, cluster):
        self.ip = ip
        self.port = port
        self.id = utils.get_id(self, ip, port)
        self._cluster: List[rpc.RemoteRaftServer] = cluster or []
        self._leader = None
        self._leader_hbeat = asyncio.Event()
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
        if isinstance(self, RaftServer):
            await rpc.handle_request(self, request)
        else:
            raise TypeError("Invalid Server instance")
        writer.close()


class RaftServer(IRaftServer, Server, RaftStateMachine):
    def __init__(
        self, ip: str, port: int, cluster, state: State = State.FOLLOWER, log=None
    ):
        Server.__init__(self, ip, port, cluster)
        RaftStateMachine.__init__(self, state, log)

        self._voted_for = None  # candidateId that received vote in currentterm
        self._next_indexes = (
            {}
        )  # for each server, index of the next log entryto send to that server
        self._match_indexes = (
            {}
        )  # for each server, index of highest log entryknown to be replicated on server
        self._commands_queue = (
            asyncio.Queue()
        )  # Queue where commands waiting for commit process to start  are stored
        # TODO: init tasks

    async def update_state(self, key, value):
        command = Command(key, value)
        if self._leader is self:
            await self._queue_command(command)
        else:
            await self._leader.commit_command(command)

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

    async def _commit_task(self):
        while True:
            command = await self._commands_queue.get()
            self._append_command(command)
            rpc_calls = list(
                map(
                    lambda s: s.append_entry(
                        self._current_term,
                        self.id,
                        self._last_applied,
                        self._log[self._last_applied].term,
                        command,
                        self._commit_index,
                    ),
                    self._cluster,
                )
            )
            committed_amount = 1  # Starts on '1' because of itself
            for rpc_call in asyncio.as_completed(*rpc_calls, timeout=RPC_TIMEOUT):
                try:
                    await rpc_call.result()
                    committed_amount += 1
                except:
                    pass
            if committed_amount >= int(len(self._cluster) * FLEXIBLE_PAXOS_QUORUM):
                self._commit_command(command)

    async def _begin_election(self):
        self._current_term += 1
        await self._change_state(State.CANDIDATE)
        last_log_index = self._last_applied
        last_log_term = 0 if not self._log else self._log[last_log_index].term
        voting_rpcs = list(
            map(
                lambda s: s.request_vote(
                    self._current_term, self.id, last_log_index, last_log_term
                ),
                self._cluster,
            )
        )
        granted_votes = 1  # 1 -> its own vote
        votes = 1
        election_win = False
        for next_vote in asyncio.as_completed(*voting_rpcs, timeout=RPC_TIMEOUT):
            try:
                vote = (await next_vote).result()
                granted_votes += int(vote)
            except:
                pass
            votes += 1
            if granted_votes >= int(
                len(self._cluster) * (1 - FLEXIBLE_PAXOS_QUORUM) + 1
            ):  # Equal because itself is not considered
                election_win = True
        if election_win:
            pass
            # TODO: change to leader

    async def _queue_command(self, command: Command):
        await self._commands_queue.put(command)
