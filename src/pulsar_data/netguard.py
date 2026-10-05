"""Outbound-network safety for pulsar-data adapters.

Security baseline (applies to every HTTP-class adapter in this repo):

* only ``http`` and ``https`` schemes may be used;
* before any request is issued the target host is resolved and every
  resolved address is checked against loopback, private, link-local,
  reserved, multicast and unspecified ranges;
* ``localhost`` and any ``*.localhost`` name is rejected by name before
  DNS is even consulted.

The check lives in :func:`validate_url`, a pure function with an
injectable resolver, so it is fully unit-testable offline.  Two carriers
are provided on top of it:

* :class:`SafeHTTPSession` — a ``requests.Session`` subclass whose every
  request (and every redirect hop) is validated first;
* :func:`install_egress_guard` — a context manager that patches
  ``requests.Session.send`` process-wide, so HTTP traffic issued by an
  upstream SDK (e.g. akshare) is validated too.

Decimal / octal / hex IPv4 forms such as ``http://2130706433/`` are
handled because validation happens on the *resolved* addresses, not on
string prefixes.
"""

from __future__ import annotations

import ipaddress
import socket
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Sequence
from urllib.parse import urljoin, urlsplit

import requests

from .errors import EgressViolation

__all__ = [
    "ALLOWED_SCHEMES",
    "validate_url",
    "host_is_forbidden",
    "SafeHTTPSession",
    "install_egress_guard",
]

#: The only schemes any adapter may issue requests with.
ALLOWED_SCHEMES = frozenset({"http", "https"})

#: Hostnames rejected by name, before any DNS lookup.
_FORBIDDEN_HOSTNAMES = frozenset({"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"})

#: Resolver type: hostname -> iterable of address strings.
Resolver = Callable[[str], Sequence[str]]

#: Positive-resolution cache: hostname -> (addresses, expiry).  DNS would
#: otherwise be queried twice per request (validate + connect).
_resolution_cache: dict[str, tuple[tuple[str, ...], float]] = {}
_RESOLUTION_TTL_SECONDS = 60.0


def _default_resolver(host: str) -> Sequence[str]:
    addresses: list[str] = []
    for info in socket.getaddrinfo(host, None):
        addresses.append(info[4][0])
    # de-duplicate, keep order
    return list(dict.fromkeys(addresses))


def _resolve(host: str) -> tuple[str, ...]:
    now = time.monotonic()
    cached = _resolution_cache.get(host)
    if cached is not None and cached[1] > now:
        return cached[0]
    try:
        addresses = tuple(_default_resolver(host))
    except OSError as exc:
        raise EgressViolation(
            f"egress refused: host {host!r} cannot be resolved: {exc}"
        ) from exc
    _resolution_cache[host] = (addresses, now + _RESOLUTION_TTL_SECONDS)
    return addresses


def _address_forbidden(ip: ipaddress._BaseAddress) -> bool:
    """True for any address class adapters must never reach."""
    return (
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or not ip.is_global
    )


def host_is_forbidden(host: str) -> bool:
    """Return True when ``host`` is a name or address adapters must never call."""
    lowered = host.strip().lower().rstrip(".")
    if lowered in _FORBIDDEN_HOSTNAMES or lowered.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(lowered)
    except ValueError:
        return False
    return _address_forbidden(ip)


def validate_url(url: str, *, resolver: Resolver | None = None) -> str:
    """Validate an outbound ``url``; return it when the target is safe.

    Raises :class:`~pulsar_data.errors.EgressViolation` when the scheme is
    not http/https, the host is forbidden by name or address, or any
    address the host resolves to is non-global (loopback, private,
    link-local, reserved, multicast, unspecified).  ``resolver`` may be
    injected for offline unit tests; by default real DNS is used.
    """
    parts = urlsplit(url)
    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        raise EgressViolation(
            f"egress refused: scheme {parts.scheme!r} is not one of {sorted(ALLOWED_SCHEMES)} ({url!r})"
        )
    host = (parts.hostname or "").strip()
    if not host:
        raise EgressViolation(f"egress refused: URL carries no host ({url!r})")
    if ":" in host:
        # unbracketed IPv6 / malformed authority — refuse rather than guess
        raise EgressViolation(
            f"egress refused: malformed host {host!r} in {url!r} (bracket IPv6 literals)"
        )
    if host_is_forbidden(host):
        raise EgressViolation(f"egress refused: host {host!r} is forbidden ({url!r})")
    if resolver is None:
        addresses: Sequence[str] = _resolve(host)
    else:
        addresses = resolver(host)
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            raise EgressViolation(
                f"egress refused: host {host!r} resolved to a non-IP token {address!r} ({url!r})"
            ) from None
        if _address_forbidden(ip):
            raise EgressViolation(
                f"egress refused: host {host!r} resolves to forbidden address {address} ({url!r})"
            )
    return url


class SafeHTTPSession(requests.Session):
    """A ``requests.Session`` that validates every request and redirect hop.

    Redirects are followed manually (auto-redirect is disabled) so a
    ``Location`` pointing at an internal host is rejected exactly like a
    direct request would be.
    """

    max_redirects = 10

    def request(self, method: str, url: str, *args: Any, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        # redirects are followed manually below so every hop is validated
        kwargs["allow_redirects"] = False
        current_method, current_url = method, url
        for _ in range(self.max_redirects + 1):
            validate_url(current_url)
            response = super().request(current_method, current_url, *args, **kwargs)
            if response.is_redirect or response.status_code in (307, 308):
                location = response.headers.get("Location")
                if not location:
                    return response
                # resolve relative redirects against the current URL
                current_url = urljoin(current_url, location)
                if response.status_code in (301, 302, 303) and current_method != "HEAD":
                    current_method = "GET"
                continue
            return response
        raise EgressViolation(
            f"egress refused: more than {self.max_redirects} redirect hops for {url!r}"
        )


@contextmanager
def install_egress_guard() -> Iterator[None]:
    """Patch ``requests.Session.send`` process-wide to validate every request.

    Use around code paths that issue HTTP through an upstream SDK whose
    session we do not control (akshare, ...).  Every request — including
    redirect hops followed inside ``requests`` — passes through
    :func:`validate_url` before any bytes are sent.  The patch is
    reverted on exit; nesting is supported.
    """
    original = requests.Session.send

    def guarded_send(self: requests.Session, prepared: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        if prepared.url is not None:
            validate_url(prepared.url)
        return original(self, prepared, **kwargs)

    if getattr(original, "_pulsar_egress_guard", False):
        yield  # already installed by an outer context
        return
    guarded_send._pulsar_egress_guard = True  # type: ignore[attr-defined]
    requests.Session.send = guarded_send  # type: ignore[method-assign]
    try:
        yield
    finally:
        requests.Session.send = original  # type: ignore[method-assign]
