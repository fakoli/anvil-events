"""Authenticated read-only artifact route for a private HTTPS front door."""

from __future__ import annotations

import hmac
import os
import re
from urllib.parse import unquote, urlsplit

from ..domain_v2 import valid_resource_identifier, valid_revision_identifier
from ..reconciliation.artifacts import (
    ArtifactUnavailable,
    DirectoryArtifactResolver,
)

_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")


class ArtifactHTTPPublisher:
    """Serve exact directory artifacts after bearer authentication.

    TLS is intentionally owned by the private ingress in front of the loopback
    health server. The publisher never logs request paths or authorization data.
    """

    def __init__(self, root, auth_env, prefix="/artifacts"):
        if not isinstance(auth_env, str) or not _ENV_NAME.fullmatch(auth_env):
            raise ValueError("artifact auth env must be a safe environment name")
        if not prefix.startswith("/") or prefix.endswith("/"):
            raise ValueError("artifact route prefix must start with one slash")
        self.resolver = DirectoryArtifactResolver(root)
        self.auth_env = auth_env
        self.prefix = prefix

    def route(self, method, target, headers):
        parsed = urlsplit(target)
        if not parsed.path.startswith(self.prefix + "/"):
            return None
        if parsed.query or parsed.fragment:
            return self._response(b"400 Bad Request")
        expected = os.environ.get(self.auth_env)
        supplied = headers.get("authorization", "")
        accepted = expected and supplied.startswith("Bearer ") and hmac.compare_digest(
            supplied[7:].encode(), expected.encode(),
        )
        if not accepted:
            return self._response(
                b"401 Unauthorized", ((b"WWW-Authenticate", b"Bearer"),),
            )
        if method != "GET":
            return self._response(b"405 Method Not Allowed", ((b"Allow", b"GET"),))
        raw = parsed.path[len(self.prefix) + 1:].split("/")
        if len(raw) != 2 or not all(raw):
            return self._response(b"404 Not Found")
        try:
            reference, revision = (unquote(value, errors="strict") for value in raw)
            if not valid_resource_identifier(reference) or not valid_revision_identifier(
                    revision):
                raise ValueError("invalid artifact identity")
            artifact = self.resolver.resolve(reference, revision)
        except (ArtifactUnavailable, ValueError, UnicodeError):
            return self._response(b"404 Not Found")
        return (
            b"200 OK",
            (
                (b"Content-Type", b"application/octet-stream"),
                (b"X-Anvil-Revision", artifact.revision.encode("ascii")),
            ),
            artifact.data,
        )

    @staticmethod
    def _response(code, headers=()):
        return code, headers, b'{"error":"artifact request refused"}'
