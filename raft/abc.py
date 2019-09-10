from abc import ABC, abstractmethod


class IRaftServer(ABC):
    @abstractmethod
    async def join_cluster(self, random_server):
        pass

    @abstractmethod
    async def leave_cluster(self):
        pass

    @abstractmethod
    async def update_state(self, key, value):
        pass
