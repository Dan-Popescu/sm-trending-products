#!/usr/bin/env python3
"""Create a private local-test configuration without changing an existing .env."""

from __future__ import annotations

import argparse
import os
import secrets
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".env"))
    args = parser.parse_args()
    password = secrets.token_hex(24)
    contents = f"""# LOCAL SYNTHETIC TESTING ONLY. Moto loses objects when restarted.
POSTGRES_USER=sm_trends
POSTGRES_DB=sm_trends
POSTGRES_PASSWORD={password}
DATABASE_URL=postgresql://sm_trends:{password}@postgres:5432/sm_trends
PIPELINE_NAMESPACE=development
S3_BUCKET=sm-trends-local
S3_ENDPOINT_URL=http://objectstore:5000
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=local-testing
AWS_SECRET_ACCESS_KEY=local-testing
CREATE_BUCKET_ON_INIT=true
TIKTOK_DISPLAY_ACCESS_TOKEN=
TIKTOK_CCL_ACCESS_TOKEN=
"""
    try:
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as destination:
            destination.write(contents)
    except FileExistsError:
        parser.exit(2, "Destination already exists; refusing to overwrite it.\n")
    print(f"Created local test configuration: {args.output}")


if __name__ == "__main__":
    main()
