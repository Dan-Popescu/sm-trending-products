-- Readers supporting plain and shared compressed assessment archives must be
-- deployed across every role before crossing this format boundary. Bridge
-- binaries continue writing plain schema-2 JSON on SQL schemas 35..37.
-- Existing derived_publications and dependency edges remain authoritative.
INSERT INTO schema_migrations(version) VALUES(38) ON CONFLICT DO NOTHING;
