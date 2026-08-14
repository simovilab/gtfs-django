"""Credential resolution for the `mc` / DuckDB-httpfs S3 backend.

Two sources, tried in order, so the same code runs unattended on the VPS (no
`mc` config there) and interactively on a laptop with `mc alias set`:

  1. an `mc` alias already configured in ``~/.mc/config.json``
  2. ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` / ``AWS_ENDPOINT_URL``
     (or the legacy ``S3_ENDPOINT``) (or
     ``MC_HOST_<alias>=https://ACCESS:SECRET@host``)

Never hardcode credentials -- this module only ever reads them from those two
places, and never logs or returns them anywhere but the returned dataclass.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import json


@dataclass(frozen=True)
class S3Credentials:
    access_key: str
    secret_key: str
    endpoint: str  # host only, no scheme
    use_ssl: bool = True


def load_credentials(alias: str, endpoint: str | None = None) -> S3Credentials:
    """Resolve credentials for `alias`.

    As a side effect, when credentials come from the environment this also
    exports ``MC_HOST_<alias>`` (if not already set) so subprocess `mc` calls
    work without a ``~/.mc/config.json`` on disk.
    """
    cfg_path = Path.home() / ".mc/config.json"
    if cfg_path.exists():
        aliases = json.loads(cfg_path.read_text()).get("aliases", {})
        a = aliases.get(alias)
        if a:
            host = urlparse(a["url"]).netloc
            return S3Credentials(a["accessKey"], a["secretKey"], host)

    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    # AWS_ENDPOINT_URL is this repo's convention (see docs/S3_LAYOUT.md and
    # rt_pipeline.storage.config.S3Config.from_env) and is what the collector's
    # .env actually sets; S3_ENDPOINT is what the standalone script this module
    # was adopted from used. Accept both, preferring the repo's own, and strip
    # a scheme if one is present since `mc`/DuckDB want a bare host here.
    host = endpoint or os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("S3_ENDPOINT")
    scheme = "https"
    if host and "://" in host:
        parsed = urlparse(host)
        scheme = parsed.scheme or scheme
        host = parsed.netloc
    mc_host = os.environ.get(f"MC_HOST_{alias}")
    if mc_host:
        u = urlparse(mc_host)
        access = access or (unquote(u.username) if u.username else None)
        secret = secret or (unquote(u.password) if u.password else None)
        host = host or (u.netloc.split("@")[-1] if u.netloc else None)
        scheme = u.scheme or scheme

    if not (access and secret and host):
        raise SystemExit(
            f"No credentials found. Either configure the mc alias "
            f"(`mc alias set {alias} https://<host> <key> <secret>`) or set "
            f"AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_ENDPOINT_URL "
            f"(or MC_HOST_{alias})."
        )

    os.environ.setdefault(
        f"MC_HOST_{alias}",
        f"{scheme}://{quote(access, safe='')}:{quote(secret, safe='')}@{host}",
    )
    return S3Credentials(access, secret, host, use_ssl=scheme.lower() != "http")
