"""Validated creator grouping shared by analytical consumers.

An identifier names a source's creator or account record, not a verified person.
Keep its source and kind separate so equal local values cannot imply equivalence.
"""

from .contracts import Observation

CreatorIdentity = tuple[str, str, str]


def creator_identity(observation: Observation) -> CreatorIdentity | None:
    """Prefer a nonblank creator string, falling back only to a valid account.

    Imported attributes may contain arbitrary JSON. Stringifying an object,
    list, number or boolean would turn malformed evidence into a known identity.
    """
    creator = observation.attributes.get("creator_id")
    if isinstance(creator, str) and creator.strip():
        return observation.source, "creator", creator.strip()
    account = observation.account_id
    if isinstance(account, str) and account.strip():
        return observation.source, "account", account.strip()
    return None
