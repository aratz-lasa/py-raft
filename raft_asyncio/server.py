import asyncio
from typing import List

from . import utils
from .rpc import rpc
from .rpc import protocol as prot
from .abc import IRaftServer
from .state_machine import RaftStateMachine, State, Command

ELECTION_TIMEOUT = 0.5
FLEXIBLE_PAXOS_QUORUM = 2 / 6
RPC_TIMEOUT = 1


class ClusterMember:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.id = utils.get_id(self, ip, port)


class Server(ClusterMember):
    def __init__(self, ip: str, port: int, cluster):
        super().__init__(ip, port)
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
        request = prot.read_decode_msg(reader, writer)
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
        self._election_task = None
        self._commit_task = None
        self._leader_task = None
        # TODO: init tasks

    async def update_state(self, key, value):
        command = Command(key, value)
        if self._leader is self:
            await self._queue_command(command)
        else:
            await self._leader.command_request(command)

    async def join_cluster(self, random_server: ClusterMember):
        if random_server:
            remote_server = rpc.RemoteRaftServer(random_server.ip, random_server.port)
            self.cluster, leader_id = await remote_server.get_cluster_configuration()
            self.leader = list(filter(lambda s: s.id == leader_id, self.cluster))[0]

            self.cluster.append(self)
            # TODO: init configuration change
        else:
            pass  # TODO: first cluster member

    async def leave_cluster(self):
        self.cluster.remove(self)
        # TODO: init configuration change

    async def remove_cluster_member(self, id):
        self.cluster = list(filter(lambda s: s.id != id, self.cluster))
        # TODO: init configuration change

    async def _run_timeout_task(self):
        while True:
            try:
                await asyncio.wait_for(
                    self._leader_hbeat.wait(), timeout=ELECTION_TIMEOUT
                )  # TODO: random timeout
            except asyncio.TimeoutError:
                await self._change_state(State.CANDIDATE)
            finally:
                self._leader_hbeat.clear()

    async def _run_commit_task_(self):
        while True:
            command = await self._commands_queue.get()
            self._append_command(command)
            rpc_calls = list(
                map(
                    lambda s: s.append_entries(
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

    async def _run_leader_task(self):
        while True:
            await asyncio.sleep(
                ELECTION_TIMEOUT * 0.9
            )  # Just in case there is high latency
            await self._send_hbeat()

    async def _run_election_task(self):
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

    async def _send_hbeat(self):
        for s in filter(lambda s: s != self, self.cluster):
            asyncio.create_task(
                s.append_entries(
                    self._current_term,
                    self.id,
                    self._last_applied,
                    self._log[self._last_applied].term,
                    None,
                    self._commit_index,
                )
            )

    def _change_state(self, new_state: State):
        if new_state is State.FOLLOWER:
            if self._leader_task and not self._leader_task.cancelled():
                self._leader_task.cancel()
            if self._election_task and not self._election_task.cancelled():
                self._election_task.cancel()
            self._state = State.FOLLOWER
            pass  # TODO
        elif new_state is State.LEADER:
            self._leader_task = asyncio.create_task(self._run_leader_task())
            self._state = State.LEADER
            pass  # TODO
        elif new_state is State.CANDIDATE:
            if self._leader_task and not self._leader_task.cancelled():
                self._leader_task.cancel()
                self._state = State.CANDIDATE
            self._election_task = asyncio.create_task(self._run_election_task())
            pass  # TODO

    def _im_leader(self):
        return self._state is State.LEADER
