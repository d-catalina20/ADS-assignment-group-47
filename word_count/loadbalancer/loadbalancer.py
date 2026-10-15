from __future__ import annotations

import logging
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import rpyc


LOGGER = logging.getLogger("word_count.loadbalancer")


@dataclass(slots=True, eq=False)
class BackendServer:
    """A backend and the small amount of state used by the selection algorithm."""

    host: str
    port: int
    active_connections: int = 0
    completed_connections: int = 0
    total_response_time: float = 0.0
    failures: int = 0
    alive: bool = True

    @property
    def average_response_time(self) -> float:
        """Average connection time in seconds, or zero before first use."""

        if self.completed_connections == 0:
            return 0.0
        return self.total_response_time / self.completed_connections

    @property
    def response_time(self) -> float:
        return self.average_response_time

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def record_response_time(self, elapsed: float) -> None:
        self.total_response_time += max(0.0, elapsed)
        self.completed_connections += 1

    
def _parse_backend_addresses(value: str) -> set[BackendServer]:
    """Parse ``host:port,host:port`` from the environment."""

    servers: set[BackendServer] = set()
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            host, port_text = item.rsplit(":", 1)
        except ValueError as exc:
            raise ValueError(f"backend must be host:port: {item!r}") from exc
        if not host or not port_text.isdigit():
            raise ValueError(f"invalid backend address: {item!r}")
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise ValueError(f"invalid backend port: {port}")
        servers.add(BackendServer(host, port))
    if not servers:
        raise ValueError("at least one backend server is required")
    return servers


class LoadBalancer:
    __slots__: tuple[str, ...] = (
        "backends",
        "host",
        "port",
        "algorithm",
        "connect_timeout",
        "_state_lock",
        "_stop_event",
        "_listener",
        "_executor",
        "_health_thread",
        "health_check_interval",
        "health_check_timeout",
    )

    def __init__(
        self,
        backends: set[BackendServer],
        host: str,
        port: int,
        max_workers: int,
        algorithm: str = "least_connections",
        connect_timeout: float = 5.0,
        health_check_interval: float = 5.0,
        health_check_timeout: float = 2.0,
    ) -> None:
        if len(backends) == 0:
            raise ValueError("at least one backend server is required")
        self.backends: set[BackendServer] = backends
        self.host: str = host
        self.port: int = port
        self.algorithm: str = algorithm.lower()
        self.connect_timeout: float = connect_timeout
        self._state_lock: threading.Lock = threading.Lock()
        self._stop_event: threading.Event = threading.Event()
        self._listener: socket.socket | None = None
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="lb-client")
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        self._health_thread = None
    def select_backend(self, excluded: set[int] | None = None) -> BackendServer:
        """Reserve and return the best backend for one incoming connection."""

        excluded = excluded or set()
        with self._state_lock:
            candidates = [item for item in self.backends if item.alive and id(item) not in excluded]
            if not candidates:
                raise RuntimeError("no backend servers available")

            if self.algorithm == "least_response_time":
                backend = min(
                    candidates,
                    key=lambda item: (
                        0 if item.completed_connections == 0 else 1,
                        item.average_response_time,
                        item.active_connections,
                    ),
                )
            else:
                backend = min(
                    candidates,
                    key=lambda item: (
                        item.active_connections,
                        item.average_response_time
                        if item.completed_connections
                        else 0.0,
                    ),
                )

            backend.active_connections += 1
            return backend

    def _release_backend(self, backend: BackendServer, elapsed: float | None = None) -> None:
        with self._state_lock:
            backend.active_connections = max(0, backend.active_connections - 1)
            if elapsed is not None:
                backend.record_response_time(elapsed)

    def _pipe(self, source: socket.socket, destination: socket.socket) -> None:
        """Copy until EOF, then half-close the destination."""

        try:
            while True:
                chunk = source.recv(64 * 1024)
                if not chunk:
                    break
                destination.sendall(chunk)
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            

    def _check_backend_health(self, backend: BackendServer) -> bool:
        """Check whether the backend responds to an RPyC ping."""

        conn = None
        try:
            conn = rpyc.connect(
                backend.host,
                backend.port,
                config={
                    "sync_request_timeout": self.health_check_timeout,
                },
            )
            return conn.root.ping() == "pong"

        except Exception as exc:
            LOGGER.debug(
                "Health check failed for %s: %s",
                backend.address,
                exc,
            )
            return False

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _health_check_loop(self) -> None:
        """Periodically check every backend's health."""

        while not self._stop_event.is_set():
            for backend in self.backends:
                if self._stop_event.is_set():
                    break

                alive = self._check_backend_health(backend)

                with self._state_lock:
                    was_alive = backend.alive
                    backend.alive = alive

                if alive != was_alive:
                    if alive:
                        LOGGER.info(
                            "Backend %s is alive again",
                            backend.address,
                        )
                    else:
                        LOGGER.warning(
                            "Backend %s is dead",
                            backend.address,
                        )

            self._stop_event.wait(self.health_check_interval)

    def _proxy_connection(self, client: socket.socket, peer: tuple[str, int] | str) -> None:
        backend: BackendServer | None = None
        backend_socket: socket.socket | None = None
        started = time.perf_counter()
        try:
            attempted: set[int] = set()
            while True:
                try:
                    backend = self.select_backend(attempted)
                except RuntimeError:
                    backend = None
                    break

                attempted.add(id(backend))
                try:
                    backend_socket = socket.create_connection(
                        (backend.host, backend.port), timeout=self.connect_timeout
                    )
                    break
                except (ConnectionError, OSError) as exc:
                    with self._state_lock:
                        backend.failures += 1
                        backend.alive = False
                    LOGGER.warning("backend %s unavailable: %s", backend.address, exc)
                    self._release_backend(backend)
                    backend = None

            if backend is None or backend_socket is None:
                return
            backend_socket.settimeout(None)
            LOGGER.debug("%s -> %s", peer, backend.address)
            client_to_backend = threading.Thread(
                target=self._pipe, args=(client, backend_socket), daemon=True
            )
            backend_to_client = threading.Thread(
                target=self._pipe, args=(backend_socket, client), daemon=True
            )
            client_to_backend.start()
            backend_to_client.start()
            client_to_backend.join()
            backend_to_client.join()
        except (ConnectionError, OSError) as exc:
            if backend is not None:
                with self._state_lock:
                    backend.failures += 1
                LOGGER.warning("backend %s unavailable: %s", backend.address, exc)
        finally:
            if backend_socket is not None:
                try:
                    backend_socket.close()
                except OSError:
                    pass
            try:
                client.close()
            except OSError:
                pass
            if backend is not None and backend_socket is not None:
                self._release_backend(backend, time.perf_counter() - started)

    
    

    
    def serve_forever(self) -> None:
        """Listen and submit each client tunnel to the worker pool."""

        listener = socket.socket(socket.AF_INET6 if ":" in self.host else socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if listener.family == socket.AF_INET6:
            listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        listener.bind((self.host, self.port))
        listener.listen(256)
        listener.settimeout(1.0)
        self._listener = listener
        self._health_thread = threading.Thread(
            target=self._health_check_loop,
            name="lb-health-check",
            daemon=True,
        )
        self._health_thread.start()
        
        LOGGER.info("listening on %s:%s", self.host, self.port)
        try:
            while not self._stop_event.is_set():
                try:
                    client, peer = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        break
                    raise
                self._executor.submit(self._proxy_connection, client, peer)
        finally:
            listener.close()

            self._listener = None
            self._stop_event.set()

            if self._health_thread is not None:
                self._health_thread.join(timeout=3.0)
            self._executor.shutdown(wait=True)

    def shutdown(self) -> None:
        self._stop_event.set()
        listener = self._listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    backend_value = os.getenv("SERVER_HOSTS", "server:18861")
    algorithm = os.getenv("LB_ALGORITHM", "least_connections")
    LOGGER.info("Starting load balancer with algorithm=%s", algorithm)
    balancer = LoadBalancer(
        backends=_parse_backend_addresses(backend_value),
        host=os.getenv("LOADBALANCER_HOST", "0.0.0.0"),
        port=int(os.getenv("LOADBALANCER_PORT", os.getenv("SERVER_PORT", "18860"))),
        max_workers=int(os.getenv("LOADBALANCER_WORKERS", "128")),
        algorithm=algorithm,
    )
    try:
        balancer.serve_forever()
    except KeyboardInterrupt:
        balancer.shutdown()


if __name__ == "__main__":
    main()
