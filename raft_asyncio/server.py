import asyncio
from typing import List

from . import utils
from .abc import IRaftServer
from .errors import *
from .rpc import protocol as prot
from .rpc import rpc
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
        message = await prot.read_decode_msg(reader)
        if isinstance(self, RaftServer):
            await rpc.handle_request(self, (reader, writer), message)
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

        self._cluster_locks = (
            {}
        )  # Lock for mantaining order when several AppendEntries RPC calls are sent for same server

        self._election_task = None
        self._timeout_task = None

        self._entries_task = None
        self._hbeat_task = None

        self._append_tasks = []
        # TODO: init tasks

    async def update_state(self, key, value):
        command = Command(key, value)
        if self._leader is self:
            await self._queue_command(command)
        else:
            while True:
                try:
                    await self._leader.command_request(command)
                    break
                except TermConsistencyError as error:
                    self._current_term = error.term
                    pass  # TODO: do something else?
                except LeaderConsistencyError as error:
                    self.leader = list(
                        filter(lambda s: s.id == error.leader_id, self.cluster)
                    )[0]

    async def join_cluster(self, random_server: ClusterMember):
        if random_server:
            remote_server = rpc.RemoteRaftServer(random_server.ip, random_server.port)
            self.cluster, leader_id = await remote_server.get_cluster_configuration()
            self.leader = list(filter(lambda s: s.id == leader_id, self.cluster))[0]

            if self not in self.cluster:
                self.cluster.append(self)
                # TODO: init configuration change
            else:
                pass  # TODO: means it already was in the cluster, but it had crushed
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

    async def _run_entries_task_(self):
        command = await self._commands_queue.get()
        self._append_command(command)
        rpc_calls = list(
            map(lambda s: self._append_entry_task(s, len(self._log) - 1), self._cluster)
        )
        committed_amount = 1  # Starts on '1' because of itself
        for rpc_call in asyncio.as_completed(rpc_calls):
            await rpc_call.result()
            committed_amount += 1
            if committed_amount >= int(len(self._cluster) * FLEXIBLE_PAXOS_QUORUM):
                self._commit_command(command)
                break

    async def _run_hbeat_task(self):
        while True:
            await self._send_hbeat()
            await asyncio.sleep(
                ELECTION_TIMEOUT * 0.9
            )  # Just in case there is high latency

    async def _run_election_task(self):
        self._current_term += 1
        self._leader_hbeat.set()
        last_log_index = self._last_applied
        last_log_term = (
            0 if not len(self._log) > last_log_index else self._log[last_log_index].term
        )
        voting_rpcs = list(
            map(
                lambda s: asyncio.create_task(
                    s.request_vote(
                        self._current_term, self.id, last_log_index, last_log_term
                    )
                ),
                filter(lambda s: s is not self, self._cluster),
            )
        )
        self._append_tasks.extend(voting_rpcs)  # For cancelling when is no more Leader
        granted_votes = 1  # 1 -> its own vote
        votes = 1
        election_win = False
        for next_vote in asyncio.as_completed(voting_rpcs, timeout=RPC_TIMEOUT):
            try:
                vote = (await next_vote).result()
                granted_votes += int(vote)
            except asyncio.TimeoutError:
                pass
            votes += 1
            if granted_votes >= int(
                len(self._cluster) * (1 - FLEXIBLE_PAXOS_QUORUM) + 1
            ):  # Equal because itself is not considered
                election_win = True
        if election_win:
            self._change_state(State.LEADER)

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
            self._cancel_leader_tasks()
            self._cancel_candidate_tasks()
            self._timeout_task = asyncio.create_task(self._run_timeout_task())
            self._state = State.FOLLOWER
        elif new_state is State.LEADER:
            if self._timeout_task and not self._timeout_task.cancelled():
                self._timeout_task.cancel()
            self._hbeat_task = asyncio.create_task(self._run_hbeat_task())
            self._entries_task = asyncio.create_task(self._run_entries_task_())
            self._state = State.LEADER
        elif new_state is State.CANDIDATE:
            self._cancel_leader_tasks()
            self._cancel_candidate_tasks()
            self._election_task = asyncio.create_task(self._run_election_task())
            self._state = State.CANDIDATE

    def _cancel_leader_tasks(self):
        if self._hbeat_task and not self._hbeat_task.cancelled():
            self._hbeat_task.cancel()
        if self._entries_task and not self._entries_task.cancelled():
            self._entries_task.cancel()
        self._cancel_append_tasks()

    def _cancel_append_tasks(self):
        for append_task in self._append_tasks:
            if not append_task.cancelled():
                append_task.cancel()
        self._append_tasks.clear()

    def _cancel_candidate_tasks(self):
        if self._election_task and not self._election_task.cancelled():
            self._election_task.cancel()

    def _im_leader(self):
        return self._state is State.LEADER

    async def _append_entry_task(
        self, server: rpc.RemoteRaftServer, entries_index: int
    ):
        async with self._cluster_locks[server.id]:
            while True:
                try:
                    await server.append_entries(
                        self._current_term,
                        self.id,
                        max(entries_index - 1, 0),
                        self._log[max(entries_index - 1, 0)].term,
                        self._log[entries_index:],
                        self._commit_index,
                    )
                    break
                except TermConsistencyError as error:
                    self._current_term = error.term
                    self._change_state(State.FOLLOWER)
                    break
                except EntriesConsistencyError:
                    entries_index = max(entries_index - 1, 0)
                except:
                    pass  # Network error, so retry until it answers
