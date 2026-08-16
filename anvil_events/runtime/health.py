"""Synchronous-bind HTTP liveness/readiness server."""

from __future__ import annotations

import json
import socket
import threading


class HealthServer:
    def __init__(self, address, snapshot, stop_event, route=None):
        self.address = address
        self.snapshot = snapshot
        self.stop_event = stop_event
        self.route = route
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
            raw = b""
            while b"\r\n\r\n" not in raw and len(raw) <= 8192:
                chunk = connection.recv(8193 - len(raw))
                if not chunk:
                    break
                raw += chunk
            if len(raw) > 8192 or b"\r\n\r\n" not in raw:
                raise ValueError("invalid HTTP request")
            lines = raw.split(b"\r\n")
            request = lines[0].split()
            if len(request) != 3 or request[2] not in (b"HTTP/1.0", b"HTTP/1.1"):
                raise ValueError("invalid HTTP request line")
            method = request[0].decode("ascii")
            path = request[1].decode("ascii")
            headers = {}
            for line in lines[1:]:
                if not line:
                    break
                if line[:1] in (b" ", b"\t"):
                    raise ValueError("folded HTTP headers are not supported")
                name, separator, value = line.partition(b":")
                if not separator:
                    raise ValueError("invalid HTTP header")
                normalized = name.decode("ascii").strip().lower()
                if not normalized or normalized in headers:
                    raise ValueError("duplicate or empty HTTP header")
                headers[normalized] = value.decode("latin-1").strip()
        except Exception:
            self._send(
                connection, b"400 Bad Request",
                ((b"Content-Type", b"application/json"),),
                b'{"error":"invalid request"}',
            )
            return
        routed = self.route(method, path, headers) if self.route else None
        if routed is not None:
            code, response_headers, body = routed
            self._send(connection, code, response_headers, body)
            return
        if method != "GET":
            self._send(
                connection, b"405 Method Not Allowed",
                ((b"Content-Type", b"application/json"), (b"Allow", b"GET")),
                b'{"error":"method not allowed"}',
            )
            return
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
        self._send(
            connection, code, ((b"Content-Type", b"application/json"),), body,
        )

    @staticmethod
    def _send(connection, code, headers, body):
        try:
            encoded_headers = b"".join(
                name + b": " + value + b"\r\n" for name, value in headers
            )
            connection.sendall(
                b"HTTP/1.1 " + code
                + b"\r\n" + encoded_headers
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
