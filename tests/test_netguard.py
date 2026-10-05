"""Egress-safety unit tests: only http/https, never loopback/private/reserved.

These tests run fully offline: hostname resolution is stubbed with a
fake resolver, and no real HTTP transport is invoked.
"""

from __future__ import annotations

import pytest
import requests

from pulsar_data.errors import EgressViolation
from pulsar_data.netguard import SafeHTTPSession, install_egress_guard, validate_url

# --------------------------------------------------------------------- schemes


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "file:///etc/passwd",
        "gopher://example.com",
        "data:text/html,hello",
        "javascript:alert(1)",
        "ws://example.com/socket",
    ],
)
def test_non_http_schemes_rejected(url):
    with pytest.raises(EgressViolation, match="scheme"):
        validate_url(url, resolver=lambda host: ["93.184.216.34"])


def test_https_scheme_allowed_case_insensitive():
    assert (
        validate_url("HTTPS://example.com/path", resolver=lambda host: ["93.184.216.34"])
        == "HTTPS://example.com/path"
    )


# ---------------------------------------------------------------------- hosts

PUBLIC = ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"]


@pytest.mark.parametrize(
    "host",
    [
        "localhost",
        "LOCALHOST",
        "localhost.localdomain",
        "api.localhost",
        "127.0.0.1",
        "127.0.1.5",
        "0.0.0.0",
        "10.0.0.5",
        "10.255.255.255",
        "172.16.0.1",
        "172.31.255.254",
        "169.254.169.254",  # cloud metadata endpoint
        "192.168.1.1",
        "192.168.0.255",
        "100.64.0.1",  # CGNAT
        "192.0.0.1",  # IETF protocol assignments
        "192.0.2.10",  # TEST-NET-1
        "198.18.0.5",  # benchmarking
        "198.51.100.7",  # TEST-NET-2
        "203.0.113.9",  # TEST-NET-3
        "240.0.0.1",  # reserved
        "255.255.255.255",
        "224.0.0.1",  # multicast
        "239.255.255.250",
        "::1",
        "[::1]",
        "fe80::1",
        "fc00::1",
        "fd12:3456:789a::1",
        "ff02::1",
        "::",
        "2130706433",  # decimal form of 127.0.0.1
        "0177.0.0.1",  # octal form of 127.0.0.1
        "0x7f.0.0.1",
    ],
)
def test_forbidden_hosts_rejected(host):
    with pytest.raises(EgressViolation):
        validate_url(f"http://{host}/", resolver=_resolving(host))


def _resolving(host: str):
    """Fake resolver that answers every hostname with the host itself.

    Numeric/octal/hex IPv4 forms are normalized by the resolver
    (``getaddrinfo`` semantics), which is exactly what production does;
    emulate that for the tricky forms.
    """
    numeric = host.strip("[]")
    aliases = {
        "2130706433": "127.0.0.1",
        "0177.0.0.1": "127.0.0.1",
        "0x7f.0.0.1": "127.0.0.1",
    }
    answer = aliases.get(numeric, numeric)
    return lambda queried: [answer]


def test_hostname_resolving_to_private_ip_rejected():
    with pytest.raises(EgressViolation, match="forbidden"):
        validate_url("http://updates.internal.example/", resolver=lambda host: ["203.0.113.5"])


def test_hostname_resolving_to_mixed_ips_rejected():
    # one public + one private address -> still refused
    with pytest.raises(EgressViolation):
        validate_url(
            "http://mixed.example/", resolver=lambda host: ["93.184.216.34", "10.0.0.9"]
        )


def test_public_targets_allowed():
    for url in (
        "http://example.com/",
        "https://push2.eastmoney.com/api/qt/stock/kline/get?secid=1.600519",
        "https://finance.sina.com.cn/",
    ):
        assert validate_url(url, resolver=lambda host: PUBLIC) == url


def test_url_without_host_rejected():
    with pytest.raises(EgressViolation, match="no host"):
        validate_url("http:///path", resolver=lambda host: [])


# ------------------------------------------------------------------- sessions


class _StubAdapter(requests.adapters.HTTPAdapter):
    """Transport stub returning canned responses without any network."""

    def __init__(self, responses):
        super().__init__()
        self._responses = list(responses)
        self.requested = []

    def send(self, request, **kwargs):
        self.requested.append(request.url)
        response = self._responses.pop(0)
        response.request = request
        return response


def _response(status=200, headers=None):
    import io

    raw = io.BytesIO(b"{}")
    response = requests.Response()
    response.status_code = status
    response.raw = raw
    response.headers.update(headers or {})
    response._content = b"{}"
    return response


def test_safe_session_blocks_redirect_to_private_host(monkeypatch):
    import pulsar_data.netguard as netguard

    monkeypatch.setattr(netguard, "_resolve", lambda host: ("93.184.216.34",))
    session = SafeHTTPSession()
    adapter = _StubAdapter(
        [
            _response(302, {"Location": "http://192.168.0.10/steal"}),
        ]
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    with pytest.raises(EgressViolation):
        session.get("http://public.example/start")


def test_safe_session_follows_safe_redirects(monkeypatch):
    import pulsar_data.netguard as netguard

    monkeypatch.setattr(netguard, "_resolve", lambda host: ("93.184.216.34",))
    session = SafeHTTPSession()
    adapter = _StubAdapter(
        [
            _response(302, {"Location": "https://other.example/final"}),
            _response(200),
        ]
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    response = session.get("http://public.example/start")
    assert response.status_code == 200
    assert adapter.requested == [
        "http://public.example/start",
        "https://other.example/final",
    ]


def test_global_guard_patches_and_restores():
    original = requests.Session.send
    with install_egress_guard():
        assert requests.Session.send is not original
        session = requests.Session()
        session.mount("http://", _StubAdapter([_response(200)]))
        with pytest.raises(EgressViolation):
            session.get("http://127.0.0.1:8080/admin")
    assert requests.Session.send is original


def test_global_guard_allows_public(monkeypatch):
    import pulsar_data.netguard as netguard

    monkeypatch.setattr(netguard, "_resolve", lambda host: ("93.184.216.34",))
    with install_egress_guard():
        session = requests.Session()
        session.mount("http://", _StubAdapter([_response(200)]))
        response = session.get("http://public.example/data")
        assert response.status_code == 200


def test_global_guard_rejects_sdk_style_request(monkeypatch):
    """An upstream SDK calling requests.get under the guard is blocked pre-connect."""
    import pulsar_data.netguard as netguard

    monkeypatch.setattr(netguard, "_resolve", lambda host: ("10.1.2.3",))
    with install_egress_guard():
        with pytest.raises(EgressViolation):
            requests.get("http://intranet.example/secret", timeout=1)
