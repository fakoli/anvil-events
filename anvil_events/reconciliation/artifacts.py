"""Configured artifact sources; events carry references, never capability URLs."""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ..transport.security import host_is_loopback
from .contracts import Artifact

MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")


class ArtifactUnavailable(RuntimeError):
    """A transient source failure; the broker delivery should be retried."""


class DirectoryArtifactResolver:
    """Resolve `<root>/<logical-reference>/<revision>` for local deployments."""

    def __init__(self, root, max_bytes=MAX_ARTIFACT_BYTES):
        supplied = Path(root)
        if supplied.is_symlink() or not supplied.is_dir():
            raise ValueError("artifact root must be a real directory")
        self.root = supplied.resolve()
        self.max_bytes = max_bytes

    def resolve(self, reference, revision):
        supplied = self.root / reference / revision
        if supplied.is_symlink():
            raise ValueError("artifact must not be a symbolic link")
        candidate = supplied.resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("artifact reference escapes configured root") from exc
        try:
            details = candidate.stat()
            if not stat.S_ISREG(details.st_mode):
                raise ValueError("artifact must be a regular file")
            size = details.st_size
            if size > self.max_bytes:
                raise ValueError("artifact exceeds configured size limit")
            data = candidate.read_bytes()
        except FileNotFoundError as exc:
            raise ArtifactUnavailable("artifact is not available") from exc
        if len(data) > self.max_bytes:
            raise ValueError("artifact exceeds configured size limit")
        return Artifact(data=data, revision=revision)


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


class HTTPSArtifactResolver:
    """Fetch exact artifacts from one configured controller origin."""

    def __init__(self, base_url, *, mode="fleet", token_env=None,
                 timeout=10, max_bytes=MAX_ARTIFACT_BYTES, opener=None):
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("artifact source requires an http(s) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("artifact source URL cannot contain credentials or query data")
        if mode == "fleet" and parsed.scheme != "https":
            raise ValueError("fleet artifact sources require HTTPS")
        if mode == "development" and parsed.scheme == "http" and not host_is_loopback(
                parsed.hostname):
            raise ValueError("development HTTP artifact sources must be loopback")
        if mode not in ("development", "fleet"):
            raise ValueError("artifact source mode must be development or fleet")
        if token_env is not None and not _ENV_NAME.fullmatch(token_env):
            raise ValueError("token_env must be a safe environment-variable name")
        if mode == "fleet" and token_env is None:
            raise ValueError("fleet artifact sources require token_env authentication")
        self.base_url = base_url.rstrip("/")
        self.token_env = token_env
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.opener = opener or build_opener(_NoRedirects())

    def resolve(self, reference, revision):
        url = f"{self.base_url}/{quote(reference, safe='')}/{quote(revision, safe='')}"
        headers = {"Accept": "application/octet-stream"}
        if self.token_env:
            token = os.environ.get(self.token_env)
            if not token:
                raise ArtifactUnavailable("artifact-source credential is unavailable")
            headers["Authorization"] = f"Bearer {token}"
        request = Request(url, headers=headers, method="GET")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                length = response.headers.get("Content-Length")
                if length is not None and int(length) > self.max_bytes:
                    raise ValueError("artifact exceeds configured size limit")
                actual_revision = response.headers.get("X-Anvil-Revision")
                if not actual_revision:
                    raise ValueError("artifact response is missing X-Anvil-Revision")
                data = response.read(self.max_bytes + 1)
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise ArtifactUnavailable("artifact source request failed") from exc
        if len(data) > self.max_bytes:
            raise ValueError("artifact exceeds configured size limit")
        return Artifact(data=data, revision=actual_revision)
