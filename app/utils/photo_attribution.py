import re

_FORMATTED_ATTRIBUTION_RE = re.compile(
    r"^\(c\)\s+.+,\s+(?:some|all|no)\s+rights\s+reserved\s+\(.+\),\s+uploaded\s+by\s+.+$",
    re.IGNORECASE,
)
_LEGACY_ATTRIBUTION_RE = re.compile(
    r"^\(c\)\s+.+,\s+(?:some|all|no)\s+rights\s+reserved\s+\(.+\)$",
    re.IGNORECASE,
)


def normalize_license_display(license_code: str | None) -> str:
    raw = (license_code or "").strip().upper()
    if not raw:
        return "LICENSE NOT PROVIDED"
    if raw == "ALL-RIGHTS-RESERVED":
        return "ALL RIGHTS RESERVED"

    normalized = raw
    if normalized.startswith("CC-"):
        normalized = "CC " + normalized[3:]
    normalized = normalized.replace("-4.0", "").replace("-3.0", "").replace("-2.0", "")
    normalized = normalized.replace("-1.0", "")
    return normalized


def rights_clause(license_code: str | None) -> str:
    raw = (license_code or "").strip().upper()
    if raw == "ALL-RIGHTS-RESERVED":
        return "all rights reserved"
    if raw.startswith("CC0"):
        return "no rights reserved"
    return "some rights reserved"


def format_attribution_display(
    attribution: str | None, rights_holder: str | None, license_code: str | None
) -> str:
    uploader = (attribution or "").strip() or "unknown uploader"
    # Backward compatibility for rows that already persisted the old full credit line.
    if _FORMATTED_ATTRIBUTION_RE.match(uploader) or _LEGACY_ATTRIBUTION_RE.match(uploader):
        return uploader

    holder = (rights_holder or "").strip() or uploader
    rights_text = rights_clause(license_code)
    license_display = normalize_license_display(license_code)
    return f"(c) {holder}, {rights_text} ({license_display}), uploaded by {uploader}"
