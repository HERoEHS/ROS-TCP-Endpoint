"""Bounded client output that never evicts protocol/service frames for data."""

from collections import deque
from queue import Empty
import threading


class OutboundQueue:
    def __init__(self, maxsize, conn, halt_event):
        self.maxsize = maxsize
        self.conn = conn
        self.halt_event = halt_event
        self.wake = threading.Event()
        self.items = deque()
        self.lock = threading.Lock()

    def offer(self, data, droppable=False):
        """Return (accepted, dropped_count); never block the caller on a client."""
        dropped = 0
        with self.lock:
            if self.maxsize > 0 and len(self.items) >= self.maxsize:
                victim = next((i for i, item in enumerate(self.items) if item[1]), None)
                if victim is None:
                    return False, int(droppable)
                del self.items[victim]
                dropped = 1
            self.items.append((data, droppable))
            self.wake.set()
        return True, dropped

    def get_nowait(self):
        with self.lock:
            if not self.items:
                raise Empty
            return self.items.popleft()[0]
