class RPCError(Exception):
    pass


class ConsistencyError(Exception):
    pass


class TermConsistencyError(ConsistencyError):
    def __init__(self, term: int):
        self.term = term


class LeaderConsistencyError(ConsistencyError):
    def __init__(self, leader_id: bytes):
        self.leader_id = leader_id


class EntriesConsistencyError(ConsistencyError):
    pass
