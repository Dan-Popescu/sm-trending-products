"""Small independent operator interface for optional launch preparation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pydantic import ValidationError

from .contracts import canonical


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="sm-trends commerce",
        description="Prepare private product launches; no platform is activated.",
        epilog="Examples: sm-trends commerce template; sm-trends commerce init; "
        "sm-trends commerce save --file dossier.json --expected-revision 0 --request-id CREATE-1; "
        "sm-trends commerce show --id PRODUCT-FR; sm-trends commerce preview --id PRODUCT-FR",
    )
    commands = root.add_subparsers(dest="action", required=True)
    commands.add_parser("template", help="Print an editable JSON template without credentials")
    commands.add_parser("init", help="Explicitly initialize optional commercial tables")
    save = commands.add_parser("save", help="Save an immutable revision; replay by request ID")
    save.add_argument("--file", type=Path, required=True)
    save.add_argument("--expected-revision", type=int, required=True)
    save.add_argument("--request-id", required=True)
    for name in ("show", "preview", "list"):
        child = commands.add_parser(name, help=f"Read private dossier {name}")
        child.add_argument("--synthetic", action="store_true", help="Use isolated simulated data")
        if name == "list":
            child.add_argument("--limit", type=int, default=100)
            child.add_argument("--offset", type=int, default=0)
        else:
            child.add_argument("--id", required=True)
        if name == "preview":
            child.add_argument("--output", type=Path, help="Write HTML to a new private file")
    return root


def template() -> dict:
    return {
        "dossier_id": "CHOOSE-PRODUCT-COUNTRY",
        "product_id": "CHOOSE-EXISTING-PRODUCT",
        "country": "CHOOSE-ISO-COUNTRY",
        "currency": "CHOOSE-ISO-CURRENCY",
        "synthetic": False,
        "fulfillment_mode": "dropship",
        "title": "",
        "description": "",
        "supplier_name": "",
        "supplier_reference": "",
        "quote_expires_at": None,
        "stock_status": "unknown",
        "delivery_min_days": None,
        "delivery_max_days": None,
        "return_terms": "",
        "retail_price": None,
        "prices_include_tax": None,
        "tax_per_order": None,
        "costs": {},
        "desired_profit": "0",
        "decision": "watch",
        "note": "",
    }


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        if arguments.action == "template":
            print(canonical(template()))
            return 0
        from .catalog import Catalog
        from .commerce import LaunchDraft, LaunchService
        from .settings import Settings

        settings = Settings()  # type: ignore[call-arg]
        catalog = Catalog(settings.database_url.get_secret_value())
        try:
            service = LaunchService(catalog)
            result: dict | list
            if arguments.action == "init":
                result = service.initialize()
            elif arguments.action == "save":
                result = service.save(
                    LaunchDraft.model_validate_json(arguments.file.read_text()),
                    expected_revision=arguments.expected_revision,
                    request_id=arguments.request_id,
                )
            elif arguments.action == "list":
                result = service.dossiers(
                    synthetic=arguments.synthetic, limit=arguments.limit, offset=arguments.offset
                )
            elif arguments.action == "show":
                result = service.dossier(arguments.id, synthetic=arguments.synthetic)
            else:
                html = service.draft_page(arguments.id, synthetic=arguments.synthetic)
                if arguments.output:
                    # Exclusive creation avoids replacing an operator's existing file.
                    import os

                    descriptor = os.open(
                        arguments.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                    )
                    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                        output.write(html)
                    result = {"status": "written", "output": str(arguments.output)}
                else:
                    print(html)
                    return 0
        finally:
            catalog.close()
        print(canonical(result), flush=True)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from .commerce import LaunchError

        body: dict = {
            "error": "launch_operation_failed",
            "hint": "Check the command inputs and commerce init. "
            "Existing files are never overwritten.",
        }
        if isinstance(error, ValidationError):
            body = {
                "error": "invalid_launch_record",
                "fields": [list(item["loc"]) for item in error.errors(include_input=False)],
            }
        elif isinstance(error, LaunchError):
            body["error"] = error.code
        # Do not expose raw database errors, connection strings or arbitrary supplied content.
        print(canonical(body), file=sys.stderr)
        return 2
