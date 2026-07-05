"""Trusted-proxy helpers — shared by QoS-label gating and the Secure-cookie decision.

sluice sits behind an ingress (Traefik in the reference deployment) that
terminates TLS and forwards the original client IP in ``X-Forwarded-For``.
Two features need to trust headers that the *direct* TCP peer (the ingress)
sets, but that an end-user must not be able to forge:

1. **QoS reserved labels** (``x-sluice-client-label``).  A request that claims
   the ``interactive`` label may consume reserved gate slots.  Without a
   trust check any client could spoof the label and steal the reserve
   (WI-028).  The label is honoured only when the immediate peer is trusted.

2. **The Secure cookie attribute** (``X-Forwarded-Proto``).  Setting
   ``Secure`` on an actually-plain-HTTP origin causes the browser to drop
   the cookie silently (login loop).  ``X-Forwarded-Proto: https`` is
   honoured only when the immediate peer is trusted.

The trust list is a set of IPv4/IPv6 CIDRs supplied by the operator via
``--trusted-proxies`` (e.g. ``10.0.0.0/8,127.0.0.0/8``).  When the list is
**empty** (the default), only loopback peers are trusted — this preserves
the single-operator dev workflow (``sluice serve`` on localhost) without
opening the spoofing surface on a deployed pod whose direct peer is the
ingress but where the operator forgot to configure the allowlist.  In a
deployment, the operator *must* set ``--trusted-proxies`` to the ingress
CIDR for the QoS reserve and the Secure-cookie decision to take effect
for non-loopback peers.

This is a shell module: it reads the ASGI scope (I/O-adjacent data) and
imports stdlib only.  It is listed in ``SHELL_MODULES`` in the
import-boundary test.
"""

from __future__ import annotations

import ipaddress
from typing import Any

Scope = dict[str, Any]

_LOOPBACK_NETWORKS = frozenset(
    {
        ipaddress.ip_network("127.0.0.0/8"),
        ipaddress.ip_network("::1/128"),
        ipaddress.ip_network("::ffff:127.0.0.0/104"),  # IPv4-mapped loopback
    }
)


def parse_trusted_proxies(raw: str | None) -> frozenset[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Parse a comma-separated CIDR/IP list into a frozenset of networks.

    Bare IPs are widened to /32 (IPv4) or /128 (IPv6).  Empty / None / whitespace
    yields an empty set.  Invalid tokens raise ValueError with the offending token
    in the message so the CLI can surface a helpful error.
    """
    if not raw:
        return frozenset()
    nets: set[ipaddress.IPv4Network | ipaddress.IPv6Network] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        # strict=False so a bare host address ("10.0.0.1") is accepted as a /32.
        net = ipaddress.ip_network(token, strict=False)
        nets.add(net)
    return frozenset(nets)


def _peer_ip(scope: Scope) -> str | None:
    """Return the immediate TCP peer's IP string from the ASGI scope, or None."""
    client = scope.get("client")
    if not client:
        return None
    ip: str = client[0]
    return ip


def peer_is_trusted(
    scope: Scope,
    trusted: frozenset[ipaddress.IPv4Network | ipaddress.IPv6Network],
) -> bool:
    """True if the immediate TCP peer is in the trusted-proxy allowlist.

    Loopback peers (127.0.0.0/8, ::1/128 and the IPv4-mapped loopback
    range) are **always** trusted — this keeps ``sluice serve`` on localhost
    working regardless of the allowlist, so a developer who sets
    ``--trusted-proxies 10.0.0.0/8`` for deployment testing doesn't lose
    local access.  When the allowlist is non-empty, non-loopback peers are
    trusted only if they match a configured CIDR.  When the allowlist is
    empty, non-loopback peers are **not** trusted (fail-safe: the default
    cannot silently open the QoS spoofing surface on a deployed pod).
    """
    ip_str = _peer_ip(scope)
    if ip_str is None:
        return False
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    if any(ip in net for net in _LOOPBACK_NETWORKS):
        return True
    if trusted:
        return any(ip in net for net in trusted)
    return False


def forwarded_proto_https(scope: Scope, trusted: frozenset[ipaddress.IPv4Network | ipaddress.IPv6Network]) -> bool:
    """True iff X-Forwarded-Proto: https was set by a trusted peer.

    Unlike ``_should_set_secure`` in admin.py, this only honours the forwarded
    header when the immediate peer is trusted — closing the spoofing surface
    where a non-trusted client injects ``X-Forwarded-Proto: https`` on a
    plain-HTTP origin and triggers a silent login loop.
    """
    if not peer_is_trusted(scope, trusted):
        return False
    for k, v in scope.get("headers", []):
        if k == b"x-forwarded-proto":
            first: str = v.decode("latin-1").split(",")[0].strip().lower()
            return first == "https"
    return False