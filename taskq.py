from asyncio import Lock
from heapq import heappop, heappush, nlargest, nsmallest
from itertools import count
from queue import Empty as QueueEmpty

class TaskQ:
    REMOVED = '<removed-task>'      # placeholder for a removed task

    def __init__(self):
        self.pq = []
        self.entry_finder = {}
        self.counter = count()
        self.lock = Lock()

    def __contains__(self, task):
        return task in self.entry_finder

    async def put(self, task, priority=0):
        with await self.lock:
            if task in self.entry_finder:
                self.__remove_nolock(task)
            count = next(self.counter)
            entry = [priority, count, task]
            self.entry_finder[task] = entry
            heappush(self.pq, entry)

    async def get_nowait(self):
        with await self.lock:
            while self.pq:
                priority, count, task = heappop(self.pq)
                if task is not TaskQ.REMOVED:
                    del self.entry_finder[task]
                    return task
            raise QueueEmpty('pop from an empty priority queue')

    async def remove(self, task):
        with await self.lock:
            self.__remove_nolock(task)

    def __remove_nolock(self, task):
        entry = self.entry_finder[task]
        entry[-1] = TaskQ.REMOVED

    def peek_head(self, n):
        return nsmallest(n, self.pq)

    def peek_tail(self, n):
        return nlargest(n, self.pq)

    def __len__(self):
        return len([1 for e in self.pq if [-1] != TaskQ.REMOVED])

    def qsize(self):
        return len(self)

    def empty(self):
        return len(self) == 0
