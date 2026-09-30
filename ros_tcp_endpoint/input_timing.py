"""Measure TCP read/dispatch stalls without treating old data as fresh input."""

import time


class InputTiming:
    """Per-connection timing; no pose contents or client clock assumptions."""

    POSE_TOPICS = {'/aeirobot/teleop/endpoint_pose', '/aeirobot/teleop/human_pose'}

    def __init__(self, peer, warn, clock=time.monotonic):
        self.peer = peer
        self.warn = warn
        self.clock = clock
        self.previous_pose = {}
        self.report_at = clock()
        self.frames = 0
        self.max_read = self.max_dispatch = self.max_gap = 0.0
        self.slow_destination = ''
        self.coalesced = self.expired = 0
        self.max_queue = 0.0

    def record(self, destination, read_started, received, dispatched, processing_started=None):
        """Account for one fully received and dispatched wire frame."""
        if not self.previous_pose:
            if destination in self.POSE_TOPICS:
                self.previous_pose[destination] = received
                self.report_at = received
            return  # connection/discovery wait is not a streaming pose gap
        self.frames += 1
        self.max_read = max(self.max_read, received - read_started)
        processing_started = received if processing_started is None else processing_started
        self.max_queue = max(self.max_queue, processing_started - received)
        dispatch = dispatched - processing_started
        if dispatch > self.max_dispatch:
            self.max_dispatch = dispatch
            self.slow_destination = destination
        if destination in self.POSE_TOPICS:
            previous = self.previous_pose.get(destination)
            if previous is not None:
                self.max_gap = max(self.max_gap, received - previous)
            self.previous_pose[destination] = received
        if dispatched - self.report_at < 5.0:
            return
        if (self.previous_pose and
                (max(self.max_gap, self.max_dispatch, self.max_queue) >= .1 or self.coalesced or self.expired)):
            self.warn(
                '[TeleopTransport] peer={} frames={} pose_gap_max_ms={:.1f} '
                'wire_read_max_ms={:.1f} dispatch_max_ms={:.1f} '
                'slow_dispatch={} queue_max_ms={:.1f} coalesced={} expired={}. '
                'Large wire wait: sender/network; large dispatch: '
                'endpoint processing. Publisher count alone does not indicate fresh input.'
                .format(self.peer, self.frames, self.max_gap * 1000,
                        self.max_read * 1000, self.max_dispatch * 1000,
                        self.slow_destination, self.max_queue * 1000, self.coalesced, self.expired))
        self.report_at = dispatched
        self.frames = 0
        self.max_read = self.max_dispatch = self.max_gap = 0.0
        self.slow_destination = ''
        self.coalesced = self.expired = 0
        self.max_queue = 0.0
