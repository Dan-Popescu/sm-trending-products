"""Persist draft commands before HTTP and keep unresolved creates uncertain."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from html import escape
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..commerce import LaunchDraft, evaluate_draft
from ..commerce_providers import (
    CommerceProvider,
    ConnectionConfig,
    DraftProduct,
    PreparedProduct,
    ProviderError,
    create_provider,
)
from ..contracts import digest
from ..module_schema import initialize_module, require_module
from ._schema import MIGRATIONS


class DeliveryError(ValueError):
    """Stable operator-facing code, without private inputs or remote diagnostics."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _identifier(value: str) -> None:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or value != value.strip()
        or any(ord(c) < 32 for c in value)
    ):
        raise DeliveryError("invalid_identifier")


def connection_fingerprint(config: ConnectionConfig) -> str:
    """Bind a validated shop origin, API version and workspace, never its secrets."""
    create_provider(config)  # Validate without contacting a provider or resolving DNS.
    _identifier(config.connection_id)
    origin = urlsplit(config.base_url)
    return digest(
        {
            "connection_id": config.connection_id,
            "provider": config.provider,
            "origin": (
                origin.scheme,
                origin.hostname,
                origin.port or (443 if origin.scheme == "https" else 80),
                origin.path.rstrip("/"),
            ),
            "synthetic": config.test_mode,
            "api_version": config.api_version,
        }
    )


class CommerceDelivery:
    """One synchronous, recoverable outbox consumer for selected draft products.

    The caller supplies the configured provider at execution time. No secrets are
    stored. A target's session advisory lock spans HTTP, without an open SQL
    transaction; competing invocations fail promptly rather than holding workers.
    """

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def initialize(self) -> dict:
        try:
            require_module(self.catalog, "commerce")
        except ValueError:
            raise DeliveryError("dossiers_not_initialized") from None
        return initialize_module(self.catalog, "commerce_delivery", MIGRATIONS)

    def _require(self) -> None:
        try:
            require_module(self.catalog, "commerce_delivery")
        except ValueError:
            raise DeliveryError("not_initialized") from None

    @staticmethod
    def _configuration(provider: CommerceProvider, synthetic: bool) -> str:
        config = provider.config
        _identifier(config.connection_id)
        if not isinstance(synthetic, bool) or synthetic != config.test_mode:
            raise DeliveryError("workspace_mismatch")
        if not config.enabled:
            raise DeliveryError("connection_disabled")
        return connection_fingerprint(config)

    @contextmanager
    def _target_guard(self, connection_id: str, dossier_id: str) -> Iterator[None]:
        # Autocommit is essential: an advisory session lock does not require a
        # transaction or retain a snapshot while the provider is contacted.
        key = connection_id + ":" + dossier_id
        conninfo = self.catalog.pool.conninfo
        connection = psycopg.connect(
            conninfo() if callable(conninfo) else conninfo,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
        )
        try:
            connection.execute("SET statement_timeout='5s'")
            # Two active deliveries per database schema, across service instances.
            # Inactive contenders close immediately; capture keeps its own pool.
            for slot in range(2):
                row = connection.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(current_schema()||"
                    "':commerce-delivery-slot:'||%s,492371243)) AS locked",
                    (str(slot),),
                ).fetchone()
                if row and row["locked"]:
                    break
            else:
                raise DeliveryError("delivery_capacity_busy")
            row = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended("
                "current_schema()||':commerce-delivery-target:'||%s,492371242)) AS locked",
                (key,),
            ).fetchone()
            if not row or not row["locked"]:
                raise DeliveryError("operation_busy")
            yield
        finally:
            # This dedicated session is NEVER returned to the pool. Closing it
            # releases both advisory locks, even if an unlock query would fail.
            connection.close()

    def prepare(
        self,
        dossier_id: str,
        provider: CommerceProvider,
        expected_revision: int,
        request_id: str,
        synthetic: bool = False,
    ) -> dict:
        _identifier(dossier_id)
        _identifier(request_id)
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or not 1 <= expected_revision < 2_147_483_647
        ):
            raise DeliveryError("invalid_revision")
        self._require()
        configuration_hash = self._configuration(provider, synthetic)
        connection_id = provider.config.connection_id
        with self._target_guard(connection_id, dossier_id):
            operation, fresh = self._enqueue(
                dossier_id, provider, expected_revision, request_id, synthetic, configuration_hash
            )
            if not fresh:
                # Replaying a command is a read. Recovery is an explicit action.
                return self._public(operation)
            return self._execute(operation, provider, recovering=False)

    def _enqueue(
        self,
        dossier_id: str,
        provider: CommerceProvider,
        expected_revision: int,
        request_id: str,
        synthetic: bool,
        configuration_hash: str,
    ) -> tuple[dict, bool]:
        connection_id = provider.config.connection_id
        request_hash = digest(
            {
                "revision": expected_revision,
                "configuration_hash": configuration_hash,
                "synthetic": synthetic,
            }
        )
        with self.catalog.transaction() as connection:
            bound = connection.execute(
                "SELECT * FROM commerce_delivery_connections WHERE connection_id=%s",
                (connection_id,),
            ).fetchone()
            if bound and bound["configuration_hash"] != configuration_hash:
                raise DeliveryError("connection_identity_mismatch")
            if not bound:
                connection.execute(
                    "INSERT INTO commerce_delivery_connections VALUES(%s,%s,%s,%s) "
                    "ON CONFLICT(connection_id) DO NOTHING",
                    (connection_id, configuration_hash, provider.config.provider, synthetic),
                )
                bound = connection.execute(
                    "SELECT * FROM commerce_delivery_connections WHERE connection_id=%s",
                    (connection_id,),
                ).fetchone()
                if bound is None or bound["configuration_hash"] != configuration_hash:
                    raise DeliveryError("connection_identity_mismatch")
            replay = connection.execute(
                "SELECT * FROM commerce_delivery_operations "
                "WHERE connection_id=%s AND dossier_id=%s AND request_id=%s",
                (connection_id, dossier_id, request_id),
            ).fetchone()
            if replay:
                if replay["request_hash"] != request_hash:
                    raise DeliveryError("request_conflict")
                return replay, False
            row = connection.execute(
                "SELECT d.*,r.draft,clock_timestamp() AS evaluated_at "
                "FROM commerce_dossiers d JOIN commerce_dossier_revisions r "
                "ON r.dossier_id=d.dossier_id AND r.revision=d.revision "
                "JOIN products p ON p.product_id=d.product_id AND p.synthetic=d.synthetic "
                "WHERE d.dossier_id=%s AND d.synthetic=%s FOR SHARE OF d,p",
                (dossier_id, synthetic),
            ).fetchone()
            if row is None:
                raise DeliveryError("dossier_not_found")
            if row["revision"] != expected_revision:
                raise DeliveryError("revision_conflict")
            draft = LaunchDraft.model_validate(row["draft"])
            if evaluate_draft(draft, row["evaluated_at"])[0]["status"] != "ready":
                raise DeliveryError("dossier_not_ready")
            unresolved = connection.execute(
                "SELECT 1 FROM commerce_delivery_operations "
                "WHERE connection_id=%s AND dossier_id=%s "
                "AND state IN('pending','running','uncertain')",
                (connection_id, dossier_id),
            ).fetchone()
            if unresolved:
                raise DeliveryError("unresolved_operation")
            target = connection.execute(
                "SELECT * FROM commerce_delivery_targets WHERE connection_id=%s AND dossier_id=%s",
                (connection_id, dossier_id),
            ).fetchone()
            if target is None:
                namespace = connection.execute("SELECT current_schema() AS namespace").fetchone()
                assert namespace is not None
                stable = digest([namespace["namespace"], connection_id, dossier_id])
                connection.execute(
                    "INSERT INTO commerce_delivery_targets(connection_id,dossier_id,launch_id,sku) "
                    "VALUES(%s,%s,%s,%s)",
                    (connection_id, dossier_id, "launch:" + stable, "SM-" + stable[:40]),
                )
                target = connection.execute(
                    "SELECT * FROM commerce_delivery_targets "
                    "WHERE connection_id=%s AND dossier_id=%s",
                    (connection_id, dossier_id),
                ).fetchone()
            assert target is not None
            if expected_revision <= target["applied_revision"]:
                raise DeliveryError("revision_already_applied")
            provider_draft = DraftProduct(
                launch_id=target["launch_id"],
                sku=target["sku"],
                title=draft.title,
                description_html=escape(draft.description).replace("\n", "<br>"),
                price=format(draft.retail_price, "f"),
                currency=draft.currency,
                price_includes_tax=draft.prices_include_tax,
            )
            operation = connection.execute(
                "INSERT INTO commerce_delivery_operations(operation_id,connection_id,dossier_id,"
                "dossier_revision,request_id,request_hash,configuration_hash,provider,"
                "canonical_product_id,synthetic,state,draft,product_id) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s,%s) RETURNING *",
                (
                    uuid4().hex,
                    connection_id,
                    dossier_id,
                    expected_revision,
                    request_id,
                    request_hash,
                    configuration_hash,
                    provider.config.provider,
                    draft.product_id,
                    synthetic,
                    Jsonb(asdict(provider_draft)),
                    target["product_id"],
                ),
            ).fetchone()
            assert operation is not None
            return operation, True

    def operation(self, operation_id: str, synthetic: bool = False) -> dict:
        _identifier(operation_id)
        if not isinstance(synthetic, bool):
            raise DeliveryError("invalid_workspace")
        self._require()
        row = self.catalog.one(
            "SELECT * FROM commerce_delivery_operations WHERE operation_id=%s AND synthetic=%s",
            (operation_id, synthetic),
        )
        if row is None:
            raise DeliveryError("operation_not_found")
        return self._public(row)

    def operations(
        self,
        connection_id: str,
        dossier_id: str,
        synthetic: bool = False,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict]:
        """Find recovery identifiers after a lost UI session, without contacting a shop."""
        self._require()
        _identifier(connection_id)
        _identifier(dossier_id)
        if (
            not isinstance(synthetic, bool)
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or not 0 <= offset <= 100_000
        ):
            raise DeliveryError("invalid_page")
        rows = self.catalog.all(
            "SELECT * FROM commerce_delivery_operations WHERE connection_id=%s AND dossier_id=%s "
            "AND synthetic=%s ORDER BY created_at DESC,operation_id LIMIT %s OFFSET %s",
            (connection_id, dossier_id, synthetic, limit, offset),
        )
        return [self._public(row) for row in rows]

    def recover(
        self, operation_id: str, provider: CommerceProvider, synthetic: bool = False
    ) -> dict:
        _identifier(operation_id)
        self._require()
        configuration_hash = self._configuration(provider, synthetic)
        row = self.catalog.one(
            "SELECT o.*,c.configuration_hash AS current_configuration_hash "
            "FROM commerce_delivery_operations o "
            "JOIN commerce_delivery_connections c USING(connection_id) "
            "WHERE o.operation_id=%s AND o.synthetic=%s",
            (operation_id, synthetic),
        )
        if row is None:
            raise DeliveryError("operation_not_found")
        if (
            row["connection_id"] != provider.config.connection_id
            or row["configuration_hash"] != configuration_hash
            or row["current_configuration_hash"] != configuration_hash
        ):
            raise DeliveryError("connection_identity_mismatch")
        with self._target_guard(row["connection_id"], row["dossier_id"]):
            current = self.catalog.one(
                "SELECT * FROM commerce_delivery_operations WHERE operation_id=%s", (operation_id,)
            )
            assert current is not None
            if current["state"] in {"succeeded", "failed"}:
                return self._public(current)
            return self._execute(current, provider, recovering=current["state"] != "pending")

    def _execute(self, operation: dict, provider: CommerceProvider, recovering: bool) -> dict:
        token = uuid4().hex
        with self.catalog.transaction() as connection:
            target = connection.execute(
                "SELECT * FROM commerce_delivery_targets WHERE connection_id=%s AND dossier_id=%s",
                (operation["connection_id"], operation["dossier_id"]),
            ).fetchone()
            assert target is not None
            if target["applied_revision"] >= operation["dossier_revision"]:
                raise DeliveryError("superseded_operation")
            source = connection.execute(
                "SELECT r.draft,clock_timestamp() AS evaluated_at "
                "FROM commerce_dossier_revisions r JOIN commerce_dossiers d USING(dossier_id) "
                "JOIN products p ON p.product_id=d.product_id AND p.synthetic=d.synthetic "
                "WHERE r.dossier_id=%s AND r.revision=%s AND d.synthetic=%s",
                (operation["dossier_id"], operation["dossier_revision"], operation["synthetic"]),
            ).fetchone()
            ready = (
                source is not None
                and evaluate_draft(
                    LaunchDraft.model_validate(source["draft"]), source["evaluated_at"]
                )[0]["status"]
                == "ready"
            )
            current = connection.execute(
                "UPDATE commerce_delivery_operations SET state='running',attempt_token=%s,"
                "error_code=NULL,updated_at=clock_timestamp() WHERE operation_id=%s "
                "AND state=%s RETURNING *",
                (token, operation["operation_id"], operation["state"]),
            ).fetchone()
            if current is None:
                raise DeliveryError("operation_busy")
        if not ready:
            return self._finish(
                current,
                token,
                "uncertain" if recovering else "failed",
                error_code="dossier_not_ready",
            )
        draft = DraftProduct(**current["draft"])
        product_id = current["product_id"]
        try:
            if product_id:
                # A previously known owned ID is stronger than eventually
                # consistent search; adapter validates its ownership and DRAFT.
                prepared = provider.update_draft(product_id, draft)
            else:
                lookup = provider.lookup_draft(draft.launch_id, draft.sku)
                if lookup.state == "found" and lookup.product is not None:
                    prepared = provider.update_draft(lookup.product.product_id, draft)
                elif lookup.state == "absent" and not recovering:
                    prepared = provider.prepare_draft(draft)
                else:
                    return self._finish(
                        current,
                        token,
                        "uncertain" if recovering else "failed",
                        error_code="lookup_unresolved",
                    )
            self._validate_result(prepared, draft, provider.config)
            return self._finish(current, token, "succeeded", prepared=prepared)
        except ProviderError as error:
            code = (
                error.code
                if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", error.code)
                else "provider_rejected"
            )
            resource_id = error.resource_id
            if resource_id is not None and not self._resource_id(
                resource_id, provider.config.provider
            ):
                resource_id = None
            return self._finish(
                current,
                token,
                "uncertain" if recovering or error.ambiguous else "failed",
                error_code=code,
                resource_id=resource_id,
            )
        except Exception:
            # Unexpected failures after dispatch are not permission to repeat a create.
            return self._finish(current, token, "uncertain", error_code="provider_operation_failed")

    @staticmethod
    def _resource_id(value: object, provider: str, variant: bool = False) -> bool:
        if not isinstance(value, str):
            return False
        if provider == "woocommerce":
            return re.fullmatch(r"[1-9][0-9]{0,19}", value) is not None
        resource = "ProductVariant" if variant else "Product"
        return re.fullmatch(rf"gid://shopify/{resource}/[1-9][0-9]{{0,19}}", value) is not None

    @classmethod
    def _validate_result(
        cls, result: PreparedProduct, draft: DraftProduct, config: ConnectionConfig
    ) -> None:
        if (
            result.status != "draft"
            or result.needs_update
            or result.sku != draft.sku
            or not cls._resource_id(result.product_id, config.provider)
            or (config.provider == "shopify" and result.variant_id is None)
            or (
                result.variant_id is not None
                and not cls._resource_id(result.variant_id, config.provider, variant=True)
            )
        ):
            raise DeliveryError("invalid_provider_result")
        if result.hosted_product_url is not None:
            url = result.hosted_product_url
            if not isinstance(url, str) or len(url) > 2048 or any(ord(c) < 32 for c in url):
                raise DeliveryError("invalid_provider_result")
            supplied, expected = urlsplit(url), urlsplit(config.base_url)
            if (
                supplied.scheme != expected.scheme
                or supplied.netloc != expected.netloc
                or supplied.username
                or supplied.password
                or supplied.fragment
            ):
                raise DeliveryError("invalid_provider_result")
            if supplied.query:
                try:
                    query = parse_qs(supplied.query, strict_parsing=True)
                except ValueError:
                    raise DeliveryError("invalid_provider_result") from None
                if config.provider != "woocommerce" or query != {
                    "post_type": ["product"],
                    "p": [result.product_id],
                }:
                    raise DeliveryError("invalid_provider_result")
            if config.provider == "shopify" and not re.fullmatch(
                r"/products/[a-z0-9-]+", supplied.path
            ):
                raise DeliveryError("invalid_provider_result")

    def _finish(
        self,
        operation: dict,
        token: str,
        state: str,
        prepared: PreparedProduct | None = None,
        error_code: str | None = None,
        resource_id: str | None = None,
    ) -> dict:
        with self.catalog.transaction() as connection:
            if prepared is not None:
                connection.execute(
                    "UPDATE commerce_delivery_targets SET applied_revision=%s,product_id=%s,"
                    "variant_id=%s,hosted_product_url=%s WHERE connection_id=%s AND dossier_id=%s "
                    "AND applied_revision<%s",
                    (
                        operation["dossier_revision"],
                        prepared.product_id,
                        prepared.variant_id,
                        prepared.hosted_product_url,
                        operation["connection_id"],
                        operation["dossier_id"],
                        operation["dossier_revision"],
                    ),
                )
            row = connection.execute(
                "UPDATE commerce_delivery_operations SET state=%s,error_code=%s,"
                "product_id=COALESCE(%s,product_id),variant_id=%s,hosted_product_url=%s,"
                "updated_at=clock_timestamp() WHERE operation_id=%s AND state='running' "
                "AND attempt_token=%s RETURNING *",
                (
                    state,
                    error_code,
                    prepared.product_id if prepared else resource_id,
                    prepared.variant_id if prepared else None,
                    prepared.hosted_product_url if prepared else None,
                    operation["operation_id"],
                    token,
                ),
            ).fetchone()
            if row is None:
                raise DeliveryError("operation_claim_lost")
            return self._public(row)

    @staticmethod
    def _public(row: dict) -> dict:
        return {
            **{
                name: row[name]
                for name in (
                    "operation_id",
                    "request_id",
                    "connection_id",
                    "dossier_id",
                    "dossier_revision",
                    "synthetic",
                    "state",
                    "product_id",
                    "variant_id",
                    "hosted_product_url",
                    "error_code",
                )
            },
            "sku": row["draft"]["sku"],
            "launch_id": row["draft"]["launch_id"],
            "currency": row["draft"]["currency"],
            "provider": row["provider"],
            "canonical_product_id": row["canonical_product_id"],
            "connection_fingerprint": row["configuration_hash"],
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
        }
