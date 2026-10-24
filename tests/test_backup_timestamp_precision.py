"""LIST fractions and HTTP-date HEAD precision describe the same S3 object."""

import json

import botocore.parsers
import botocore.session
import pytest
from botocore.awsrequest import HeadersDict
from test_backup_checkpoint_cost import checkpoint
from test_object_backup import snapshot
from test_sharded_results import seed


def decoded_identity(timestamp="2026-10-24T09:20:15.123Z", **headers):
    service = botocore.session.get_session().get_service_model("s3")
    parser = botocore.parsers.create_parser("rest-xml")
    body = (
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Contents><Key>object</Key><LastModified>{timestamp}</LastModified>"
        '<ETag>"same-etag"</ETag><Size>4</Size></Contents></ListBucketResult>'
    ).encode()
    listed = parser.parse(
        {"status_code": 200, "headers": {}, "body": body},
        service.operation_model("ListObjectsV2").output_shape,
    )["Contents"][0]
    head = parser.parse(
        {
            "status_code": 200,
            "headers": HeadersDict(
                {
                    "content-length": "4",
                    "etag": '"same-etag"',
                    "last-modified": "Sat, 24 Oct 2026 09:20:15 GMT",
                    "x-amz-meta-sha256": "a" * 64,
                    "x-amz-version-id": "checkpoint-version",
                    **headers,
                }
            ),
            "body": b"",
        },
        service.operation_model("HeadObject").output_shape,
    )
    return listed, head


def inspect(monkeypatch, listed, head):
    import boto3

    s3 = boto3.client(
        "s3", aws_access_key_id="fixture", aws_secret_access_key="fixture", region_name="us-east-1"
    )

    def observed_head(**kwargs):
        assert kwargs == {"Bucket": "bucket", "Key": "object", "IfMatch": listed["ETag"]}
        return head

    monkeypatch.setattr(s3, "head_object", observed_head)
    record = snapshot["_checkpoint_record"](listed, ("a" * 64, 4))
    snapshot["_inspect_checkpoint_record"](s3, "bucket", record)
    return record


@pytest.mark.parametrize("timestamp", ["2026-10-24T09:20:15.123Z", "2026-10-24T11:20:15.987+02:00"])
def test_wire_decoded_fractional_list_matches_head_whole_utc_second(monkeypatch, timestamp):
    listed, head = decoded_identity(timestamp)
    assert listed["LastModified"].microsecond and head["LastModified"].microsecond == 0
    record = inspect(monkeypatch, listed, head)
    assert record["last_modified"] == listed["LastModified"].isoformat()
    assert record["version_id"] == "checkpoint-version"
    assert record["expected_sha256"] == record["metadata"]["sha256"] == "a" * 64


@pytest.mark.parametrize(
    "headers",
    [
        {"last-modified": "Sat, 24 Oct 2026 09:20:14 GMT"},
        {"last-modified": "Sat, 24 Oct 2026 09:20:16 GMT"},
        {"etag": '"different-etag"'},
        {"content-length": "5"},
        {"x-amz-meta-sha256": "b" * 64},
    ],
)
def test_wire_decoded_changed_second_or_identity_still_fails_closed(monkeypatch, headers):
    listed, head = decoded_identity(**headers)
    with pytest.raises(ValueError, match="changed before metadata|checksum metadata mismatch"):
        inspect(monkeypatch, listed, head)


@pytest.mark.integration
def test_fractional_list_fingerprint_change_requires_fresh_head(pipeline, tmp_path, monkeypatch):
    seed(pipeline, videos=1, points=2)
    s3, gets, state, options = checkpoint(pipeline, tmp_path, monkeypatch)
    original = s3.list_objects_v2
    fraction = 123000

    def fractional_listing(**kwargs):
        response = original(**kwargs)
        for item in response.get("Contents", []):
            item["LastModified"] = item["LastModified"].replace(microsecond=fraction)
        return response

    monkeypatch.setattr(s3, "list_objects_v2", fractional_listing)
    base, unchanged, changed = (
        tmp_path / name / "objects" for name in ("base", "unchanged", "changed")
    )
    first = snapshot["capture"](base, copy_objects=True, **options)
    assert first["complete"] and first["object_count"] > 0
    assert first["head_requests"] == first["object_count"]
    gets.clear()
    state["heads"] = 0
    second = snapshot["capture"](unchanged, copy_objects=True, base=base, **options)
    assert second["catalog_reused_object_count"] == second["object_count"]
    assert second["head_requests"] == state["heads"] == len(gets) == 0
    fraction = 456000
    third = snapshot["capture"](changed, copy_objects=True, base=unchanged, **options)
    assert third["complete"] and third["catalog_reused_object_count"] == 0
    assert third["head_requests"] == state["heads"] == third["object_count"]
    assert third["copied_object_count"] == len(gets) == 0
    for directory, expected in ((base, ".123000+00:00"), (changed, ".456000+00:00")):
        records = [
            json.loads(line) for line in (directory / "objects.jsonl").read_text().splitlines()
        ]
        assert all(record["last_modified"].endswith(expected) for record in records)
    assert snapshot["verify"](changed)["complete"]
