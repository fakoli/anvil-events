"""No-downgrade endpoint, authentication, and TLS policy."""

from __future__ import annotations

import ipaddress
import os
import re
import ssl
from dataclasses import dataclass
from urllib.parse import urlsplit


def parse_endpoint(url):
    value = url or "nats://127.0.0.1:4222"
    parsed = urlsplit(value)
    if parsed.scheme not in ("nats", "tls"):
        raise ValueError("NATS URL scheme must be nats:// or tls://")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credentials are forbidden in NATS URLs")
    if parsed.query or parsed.fragment or parsed.path not in ("", "/"):
        raise ValueError("NATS URL must not contain a path, query, or fragment")
    if not parsed.hostname:
        raise ValueError("NATS URL requires a host")
    try:
        port = parsed.port or 4222
    except ValueError as exc:
        raise ValueError("NATS URL has an invalid port") from exc
    if not 1 <= port <= 65535:
        raise ValueError("NATS URL port must be between 1 and 65535")
    return parsed.scheme, parsed.hostname, port


def parse_url(url):
    _, host, port = parse_endpoint(url)
    return host, port


def host_is_loopback(host):
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True, repr=False)
class SecurityConfig:
    mode: str
    username: str | None
    password: str | None
    token: str | None
    ca_file: str | None
    cert_file: str | None
    key_file: str | None
    server_name: str | None
    handshake_first: bool
    development_hosts: frozenset[str]

    @classmethod
    def from_environment(cls, *, mode=None, username=None, password=None,
                         token=None, ca_file=None, cert_file=None,
                         key_file=None, server_name=None,
                         handshake_first=None, development_hosts=None):
        if handshake_first is None:
            raw_handshake = os.environ.get(
                "ANVIL_EVENTS_TLS_HANDSHAKE_FIRST", "false",
            ).lower()
            if raw_handshake not in ("true", "false"):
                raise ValueError(
                    "ANVIL_EVENTS_TLS_HANDSHAKE_FIRST must be true or false"
                )
            handshake_first = raw_handshake == "true"
        elif not isinstance(handshake_first, bool):
            raise ValueError("handshake_first must be a boolean")
        if development_hosts is None:
            development_hosts = os.environ.get(
                "ANVIL_EVENTS_DEVELOPMENT_PLAINTEXT_HOSTS", "",
            ).split(",")
        if isinstance(development_hosts, str):
            development_hosts = development_hosts.split(",")
        development_hosts = frozenset(
            host.strip() for host in development_hosts if host.strip()
        )
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", host)
               for host in development_hosts):
            raise ValueError(
                "development plaintext hosts must be single-label DNS tokens"
            )
        return cls(
            mode or os.environ.get("ANVIL_EVENTS_TRANSPORT_MODE", "development"),
            username or os.environ.get("ANVIL_EVENTS_NATS_USERNAME"),
            password or os.environ.get("ANVIL_EVENTS_NATS_PASSWORD"),
            token or os.environ.get("ANVIL_EVENTS_NATS_TOKEN"),
            ca_file or os.environ.get("ANVIL_EVENTS_TLS_CA_FILE"),
            cert_file or os.environ.get("ANVIL_EVENTS_TLS_CERT_FILE"),
            key_file or os.environ.get("ANVIL_EVENTS_TLS_KEY_FILE"),
            server_name or os.environ.get("ANVIL_EVENTS_TLS_SERVER_NAME"),
            handshake_first,
            development_hosts,
        )

    def validate(self, url):
        scheme, host, port = parse_endpoint(url)
        if self.mode not in ("development", "fleet"):
            raise ValueError("transport mode must be development or fleet")
        if self.token and (self.username or self.password):
            raise ValueError("configure token auth or username/password, not both")
        if bool(self.username) != bool(self.password):
            raise ValueError("username and password must be configured together")
        if bool(self.cert_file) != bool(self.key_file):
            raise ValueError("TLS certificate and key must be configured together")
        if self.mode == "development":
            if (scheme == "nats" and not host_is_loopback(host)
                    and host not in self.development_hosts):
                raise ValueError(
                    "plaintext NATS requires loopback or an explicit single-label "
                    "development host"
                )
        else:
            if scheme != "tls":
                raise ValueError("fleet transport requires a tls:// endpoint")
            if not ((self.username and self.password)
                    or (self.cert_file and self.key_file)):
                raise ValueError(
                    "fleet transport requires username/password or a client certificate"
                )
        return scheme, host, port

    def tls_context(self):
        context = ssl.create_default_context(cafile=self.ca_file)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        if self.cert_file:
            context.load_cert_chain(self.cert_file, self.key_file)
        return context

    def connect_options(self, tls_enabled):
        options = {
            "verbose": False,
            "pedantic": False,
            "tls_required": tls_enabled,
            "headers": True,
            "name": "anvil-events",
        }
        if self.token:
            options["auth_token"] = self.token
        elif self.username:
            options["user"] = self.username
            options["pass"] = self.password
        return options
