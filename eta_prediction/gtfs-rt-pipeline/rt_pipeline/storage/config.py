"""S3/MinIO connection config, sourced from environment variables.

Never hardcode credentials. Reads (in order of precedence):
  - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
  - AWS_ENDPOINT_URL (or S3_ENDPOINT_URL)
  - AWS_REGION / AWS_DEFAULT_REGION (default: us-east-1)

For the SIMOVI MinIO server these map to the values documented in
SIMOVILAB-S3.md (which must stay out of version control).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    endpoint: str  # host only, no scheme (e.g. "data.simovilab.org")
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    use_ssl: bool = True
    url_style: str = "path"

    @classmethod
    def from_env(cls) -> "S3Config":
        endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get(
            "S3_ENDPOINT_URL"
        )
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        region = (
            os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or "us-east-1"
        )
        missing = [
            name
            for name, val in (
                ("AWS_ENDPOINT_URL", endpoint),
                ("AWS_ACCESS_KEY_ID", access_key),
                ("AWS_SECRET_ACCESS_KEY", secret_key),
            )
            if not val
        ]
        if missing:
            raise RuntimeError(
                "Missing S3 environment variables: " + ", ".join(missing)
            )

        use_ssl = True
        host = endpoint
        if "://" in endpoint:
            scheme, host = endpoint.split("://", 1)
            use_ssl = scheme.lower() == "https"
        host = host.rstrip("/")

        return cls(
            endpoint=host,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            use_ssl=use_ssl,
        )
