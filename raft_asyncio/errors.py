class RPCError(Exception):
    pass


class ConsistencyError(Exception):
    pass


class TermConsistencyError(ConsistencyError):
    def __init__(self, term: int):
        self.term = term


class EntriesConsistencyError(ConsistencyError):
    pass
