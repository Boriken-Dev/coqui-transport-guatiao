"""A transport written in Python: connections inside one process, as a provider of the `transport` kind.

The module is the library: its id is its name, its version `__version__`, its providers the classes in
`__all__`. An endpoint is `SCHEME:NAME` (the scheme is a setting, `memory` by default); a dialler finds the
listener of the same name, and each connection is a pair of queues. It keeps every rule of the kind: `send`
waits for room and `close` wakes it; one thread of its own per connection is the only caller of the sink; what was
sent comes before the end; the end is told once; a listener's own thread is the only caller of its acceptor.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

from coqui_transport_guatiao_kinds import Channel, ChannelSink, Transport, TransportAcceptor, TransportListener

from guatiao import Ref, provide
from guatiao.errors import BadValue, Gone

__version__ = "1.0.0"
__all__ = ["Memory"]

OK = 0
BAD_VALUE = 7
GONE = 12
INTERNAL = 100

#: How many messages wait for the reader before `send` waits too.
QUEUE = 4

DIAL_KEYS = {"endpoint", "timeout_ms"}
LISTEN_KEYS = {"endpoint"}


class _Side:
    """One end of a connection: what is waiting for it, and how it ended."""

    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.items: Deque[bytes] = deque()
        #: This end was closed, by the host or because its sink refused: the status and reason.
        self.closed: Optional[Tuple[int, str]] = None
        #: The other end was closed.
        self.peer_closed = False
        self.peer: Optional["_Side"] = None

    def close(self, status: int, reason: str) -> None:
        with self.cond:
            if self.closed is not None:
                return
            self.closed = (status, reason)
            self.items.clear()
            self.cond.notify_all()
        peer = self.peer
        if peer is not None:
            with peer.cond:
                peer.peer_closed = True
                peer.cond.notify_all()


class _Connection(Channel):
    """The host's hold on one end. Dropping it closes."""

    def __init__(self, side: _Side, max_message: int) -> None:
        self._side = side
        self._max = max_message

    def send(self, bytes: bytes) -> None:
        mine = self._side
        if len(bytes) > self._max:
            raise BadValue("the message is larger than this transport takes")
        peer = mine.peer
        assert peer is not None
        with peer.cond:
            while True:
                if mine.closed is not None or peer.closed is not None or peer.peer_closed:
                    raise Gone("the connection is closed")
                if len(peer.items) < QUEUE:
                    break
                peer.cond.wait(timeout=0.05)
            peer.items.append(bytes)
            peer.cond.notify_all()

    def close(self, status: int, reason: str) -> None:
        self._side.close(status, reason)

    def __del__(self) -> None:
        self._side.close(OK, "")


def _pair() -> Tuple[_Side, _Side]:
    a, b = _Side(), _Side()
    a.peer, b.peer = b, a
    return a, b


def _read(side: _Side, sink: ChannelSink) -> None:
    """The one thread that calls the sink: messages in order, then the end, once."""
    status: Tuple[int, str] = (OK, "")
    while True:
        message: Optional[bytes] = None
        with side.cond:
            while not side.items and side.closed is None and not side.peer_closed:
                side.cond.wait()
            if side.closed is not None:
                status = side.closed
                break
            if side.items:
                message = side.items.popleft()
                side.cond.notify_all()
            else:
                status = (OK, "")
                break
        try:
            sink.receive(message)  # type: ignore[arg-type]
        except Exception as e:  # a sink that refuses ends the connection
            status = (int(getattr(e, "status", INTERNAL)), str(e))
            side.close(*status)
            break
    sink.closed(status[0], status[1])


def _start(side: _Side, sink: ChannelSink) -> None:
    threading.Thread(target=_read, args=(side, sink), name="memory_transport_reader", daemon=True).start()


class _Listening(TransportListener):
    def __init__(self, endpoint: str, stop: "_Stop") -> None:
        self._endpoint = endpoint
        self._stop = stop

    def address(self) -> str:
        return self._endpoint

    def close(self, status: int, reason: str) -> None:
        self._stop.set(status, reason)

    def __del__(self) -> None:
        self._stop.set(OK, "")


class _Stop:
    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.value: Optional[Tuple[int, str]] = None

    def set(self, status: int, reason: str) -> None:
        with self.cond:
            if self.value is None:
                self.value = (status, reason)
            self.cond.notify_all()


#: The endpoints being listened on, and what is waiting to be accepted on each.
_LISTENERS: Dict[str, Tuple[Deque[_Side], threading.Condition]] = {}
_LOCK = threading.Lock()


def _accept(endpoint: str, acceptor: TransportAcceptor, stop: _Stop, max_message: int) -> None:
    """The listener's thread, the only caller of the acceptor."""
    with _LOCK:
        waiting, cond = _LISTENERS[endpoint]
    status: Tuple[int, str] = (OK, "")
    while True:
        with cond:
            while not waiting and stop.value is None:
                cond.wait(timeout=0.05)
            if stop.value is not None:
                status = stop.value
                break
            side = waiting.popleft()
        try:
            sink = acceptor.accepted("memory", _Connection(side, max_message))
        except Exception:
            side.close(OK, "")
            continue
        _start(side, sink)
    # The endpoint is free before the host is told it is closed.
    with _LOCK:
        _LISTENERS.pop(endpoint, None)
    acceptor.closed(status[0], status[1])


@dataclass(frozen=True)
class MemoryConfig:
    """How the memory transport is configured."""

    #: The largest message, in bytes.
    max_message: int = 1024
    #: What an endpoint starts with, before the colon.
    scheme: str = "memory"


class Memory(provide.Provider, Transport, display_name="Memory, in Python", default=True):
    """Connections inside one process."""

    def __init__(self, config: MemoryConfig = MemoryConfig()) -> None:
        if config.max_message < 1:
            raise BadValue("`max_message` is at least 1")
        self.config = config

    def _endpoint(self, request: Ref, keys: set) -> str:
        asked = request.to_python()
        if not isinstance(asked, dict) or not set(asked) <= keys or not isinstance(asked.get("endpoint"), str):
            raise BadValue("the request is not one this transport reads")
        endpoint: str = asked["endpoint"]
        if not endpoint.startswith(self.config.scheme + ":") or len(endpoint) == len(self.config.scheme) + 1:
            raise BadValue("the endpoint is not SCHEME:NAME")
        return endpoint

    def describe(self) -> Dict[str, Any]:
        return {
            "title": "Memory, in Python",
            "endpoint": self.config.scheme + ":NAME",
            "dials": True,
            "listens": True,
            "max_message_bytes": self.config.max_message,
        }

    def dial(self, request: Ref, sink: ChannelSink) -> Channel:
        endpoint = self._endpoint(request, DIAL_KEYS)
        with _LOCK:
            found = _LISTENERS.get(endpoint)
        if found is None:
            raise Gone("nobody listens there")
        ours, theirs = _pair()
        waiting, cond = found
        with cond:
            waiting.append(theirs)
            cond.notify_all()
        _start(ours, sink)
        return _Connection(ours, self.config.max_message)

    def listen(self, request: Ref, acceptor: TransportAcceptor) -> TransportListener:
        endpoint = self._endpoint(request, LISTEN_KEYS)
        cond = threading.Condition()
        with _LOCK:
            if endpoint in _LISTENERS:
                raise Gone("that endpoint is taken")
            _LISTENERS[endpoint] = (deque(), cond)
        stop = _Stop()
        threading.Thread(
            target=_accept,
            args=(endpoint, acceptor, stop, self.config.max_message),
            name="memory_transport_accept",
            daemon=True,
        ).start()
        return _Listening(endpoint, stop)

    def non_defaults(self) -> List[Dict[str, str]]:
        defaults = MemoryConfig()
        listed: List[Dict[str, str]] = []
        if self.config.max_message != defaults.max_message:
            listed.append(
                {
                    "setting": "max_message",
                    "value": f"usize({self.config.max_message})",
                    "default": f"usize({defaults.max_message})",
                }
            )
        if self.config.scheme != defaults.scheme:
            listed.append(
                {
                    "setting": "scheme",
                    "value": f"String({self.config.scheme})",
                    "default": f"String({defaults.scheme})",
                }
            )
        return listed
