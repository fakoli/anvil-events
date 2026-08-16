"""Synchronous-bind HTTP liveness/readiness server."""

from __future__ import annotations

import json
import socket
import threading


class HealthServer:
    def __init__(self, address, snapshot, stop_event):
        self.address = address
        self.snapshot = snapshot
        self.stop_event = stop_event
        self.socket = None
        self.thread = None
        self._active = None
        self._active_lock = threading.Lock()

    def start(self):
        if self.thread is not None:
            raise RuntimeError("health server already started")
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(self.address)
            server.listen(4)
            server.settimeout(0.5)
        except Exception:
            server.close()
            raise
        self.socket = server
        self.address = server.getsockname()
        self.thread = threading.Thread(
            target=self._serve, name="event-health", daemon=False,
        )
        self.thread.start()
        return self.thread

    def _serve(self):
        while not self.stop_event.is_set():
            try:
                connection, _ = self.socket.accept()
                with self._active_lock:
                    self._active = connection
                try:
                    self._respond(connection)
                finally:
                    with self._active_lock:
                        if self._active is connection:
                            self._active = None
            except TimeoutError:
                continue
            except OSError:
                if self.stop_event.is_set():
                    break

    def _respond(self, connection):
        connection.settimeout(0.5)
        try:
            request = connection.recv(4096).split(b"\r\n", 1)[0].split()
            path = request[1].decode("ascii") if len(request) >= 2 else "/"
        except Exception:
            path = "/"
        status = self.snapshot()
        if path not in ("/", "/live", "/ready"):
            code = b"404 Not Found"
        elif path == "/ready" and not status["fleet_delivery_ready"]:
            code = b"503 Service Unavailable"
        elif path == "/live" and not status["local_accepting"]:
            code = b"503 Service Unavailable"
        else:
            code = b"200 OK"
        body = json.dumps(status, sort_keys=True).encode()
        try:
            connection.sendall(
                b"HTTP/1.1 " + code
                + b"\r\nContent-Type: application/json\r\n"
                + b"Content-Length: " + str(len(body)).encode()
                + b"\r\nConnection: close\r\n\r\n" + body
            )
        finally:
            connection.close()

    def close(self, timeout=5):
        self.stop_event.set()
        if self.socket is not None:
            try:
                self.socket.close()
            except OSError:
                pass
        with self._active_lock:
            active = self._active
        if active is not None:
            try:
                active.close()
            except OSError:
                pass
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join(timeout)
            if self.thread.is_alive():
                raise RuntimeError("health server did not stop")
