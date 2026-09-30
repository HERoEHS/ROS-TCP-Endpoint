"""Bounded buffered TCP framing and latest-value teleop burst handling."""

from collections import deque
from dataclasses import dataclass
import select
import struct
import time


# Only state streams may be superseded. ON/OFF, recording, syscommands and
# services remain ordered, lossless barriers between groups of state frames.
LATEST_INPUT_TOPICS = frozenset({
    '/aeirobot/teleop/endpoint_pose', '/aeirobot/teleop/human_pose',
    '/aeirobot/teleop/left_hand', '/aeirobot/teleop/right_hand',
    '/aeirobot/vr/cmd_vel',
})


@dataclass(frozen=True)
class Frame:
    destination: str
    data: bytes
    received_at: float


class FrameReader:
    """Read available bytes in bounded bursts, without a batching sleep."""

    MAX_NAME_SIZE = 4096
    MAX_MESSAGE_SIZE = 100 * 1024 * 1024
    CHUNK_SIZE = 16384
    MAX_BATCH_BYTES = 65536
    MAX_BATCH_FRAMES = 256
    MAX_DRAIN_SECONDS = .001

    def __init__(self, conn):
        self.conn = conn
        self.buffer = bytearray()
        self.ready = deque()
        self.read_calls = 0

    def feed(self, data, received_at):
        """Preserve the completion time even if dispatch or framing is delayed."""
        self.buffer.extend(data)
        offset = 0
        while len(self.buffer) - offset >= 4:
            name_size = struct.unpack_from('<I', self.buffer, offset)[0]
            if name_size > self.MAX_NAME_SIZE:
                raise ValueError('TCP destination exceeds 4096 bytes')
            header_end = offset + 4 + name_size + 4
            if len(self.buffer) < header_end:
                break
            message_size = struct.unpack_from('<I', self.buffer, header_end - 4)[0]
            if message_size > self.MAX_MESSAGE_SIZE:
                raise ValueError('TCP payload exceeds 100 MiB')
            end = header_end + message_size
            if len(self.buffer) < end:
                break
            destination = bytes(self.buffer[offset+4:header_end-4]).decode('utf-8').rstrip('\0')
            self.ready.append(Frame(destination, bytes(self.buffer[header_end:end]), received_at))
            offset = end
        if offset:
            del self.buffer[:offset]

    def read_batch(self):
        """Block for one complete frame, then drain only already available data."""
        while not self.ready:
            data = self.conn.recv(self.CHUNK_SIZE)
            self.read_calls += 1
            if not data:
                raise IOError('No more data available')
            self.feed(data, time.monotonic())
        deadline = time.monotonic() + self.MAX_DRAIN_SECONDS
        drained = 0
        while (len(self.ready) < self.MAX_BATCH_FRAMES and drained < self.MAX_BATCH_BYTES
               and time.monotonic() < deadline):
            if not select.select([self.conn], [], [], 0)[0]:
                break
            data = self.conn.recv(self.CHUNK_SIZE)
            self.read_calls += 1
            if not data:
                break  # deliver complete frames before handling EOF next time
            drained += len(data)
            self.feed(data, time.monotonic())
        return [self.ready.popleft() for _ in range(min(len(self.ready), self.MAX_BATCH_FRAMES))]


def latest_frames(frames, pending_service=False):
    """Keep latest state per topic without crossing event/service boundaries."""
    pending = {}
    result = []
    for frame in frames:
        if frame.destination in LATEST_INPUT_TOPICS and not pending_service:
            pending.pop(frame.destination, None)
            pending[frame.destination] = frame
            continue
        result.extend(pending.values())  # order of the last wire occurrence
        pending.clear()
        result.append(frame)
        if pending_service:
            pending_service = False
        elif frame.destination in ('__request', '__response'):
            pending_service = True
    result.extend(pending.values())
    return result
