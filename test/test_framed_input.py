"""Fragmentation, burst ordering and bounded framing regressions."""

import socket
import struct
import time

import pytest

from ros_tcp_endpoint.framed_input import Frame, FrameReader, latest_frames


POSE = '/aeirobot/teleop/endpoint_pose'
HAND = '/aeirobot/teleop/left_hand'
ONOFF = '/aeirobot/teleop/onoff'


def wire(name, payload):
    encoded = name.encode()
    return struct.pack('<I', len(encoded)) + encoded + struct.pack('<I', len(payload)) + payload


def test_fragmented_frames_keep_completion_timestamp():
    reader = FrameReader(None)
    data = wire(POSE, b'pose') + wire(HAND, b'hand')
    for i, value in enumerate(data):
        reader.feed(bytes([value]), float(i))
    frames = list(reader.ready)
    assert [(f.destination, f.data) for f in frames] == [(POSE, b'pose'), (HAND, b'hand')]
    assert frames[0].received_at == len(wire(POSE, b'pose')) - 1
    assert frames[1].received_at == len(data) - 1
    assert not reader.buffer


def test_partial_next_frame_does_not_delay_complete_pose():
    host, peer = socket.socketpair()
    host.settimeout(.2)
    try:
        reader = FrameReader(host)
        following = wire(HAND, b'next')
        peer.sendall(wire(POSE, b'current') + following[:5])
        start = time.monotonic()
        batch = reader.read_batch()
        assert time.monotonic() - start < .1
        assert [(f.destination, f.data) for f in batch] == [(POSE, b'current')]
        peer.sendall(following[5:])
        assert reader.read_batch()[0].data == b'next'
    finally:
        host.close()
        peer.close()


@pytest.mark.parametrize('payload', [
    struct.pack('<I', FrameReader.MAX_NAME_SIZE + 1),
    struct.pack('<II', 0, FrameReader.MAX_MESSAGE_SIZE + 1),
])
def test_oversized_header_fails_before_payload_allocation(payload):
    reader = FrameReader(None)
    with pytest.raises(ValueError):
        reader.feed(payload, 0.)
    assert len(reader.buffer) < 16


def test_utf8_name_and_empty_keepalive():
    reader = FrameReader(None)
    reader.feed(wire('/상태\0', b'abc') + wire('', b''), 1.)
    assert [(f.destination, f.data) for f in reader.ready] == [('/상태', b'abc'), ('', b'')]


def test_bursts_coalesce_per_topic_but_preserve_onoff_and_recording_order():
    frames = [Frame(POSE, b'old', 0.), Frame(HAND, b'old_hand', .001),
              Frame(POSE, b'before_on', .002), Frame(ONOFF, b'ON', .003),
              Frame(POSE, b'after_on_old', .004), Frame(POSE, b'after_on', .005),
              Frame('/aeirobot/alice/vr/data_record', b'start', .006),
              Frame(ONOFF, b'OFF', .007), Frame(POSE, b'last', .008)]
    assert [f.data for f in latest_frames(frames)] == [
        b'old_hand', b'before_on', b'ON', b'after_on', b'start', b'OFF', b'last']


@pytest.mark.parametrize('header', ['__request', '__response'])
def test_service_body_is_never_coalesced_even_when_named_like_a_state_topic(header):
    frames = [Frame(header, b'id', 0.), Frame(POSE, b'service_body', .001),
              Frame(POSE, b'old_pose', .002), Frame(POSE, b'latest_pose', .003)]
    assert [f.data for f in latest_frames(frames)] == [b'id', b'service_body', b'latest_pose']
    assert [f.data for f in latest_frames(frames[1:], pending_service=True)] == [
        b'service_body', b'latest_pose']


def test_batch_bound_and_timestamps_survive_buffered_remainder():
    reader = FrameReader(None)
    reader.feed(wire(POSE, b'x') * 1000, 3.)
    # Enough complete frames are already buffered, so no socket access is needed.
    frames = reader.read_batch()
    assert len(frames) == FrameReader.MAX_BATCH_FRAMES
    assert all(f.received_at == 3. for f in frames)
    assert len(reader.ready) == 1000 - len(frames)
