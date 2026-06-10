"""
Audit active audience signal configurations against default setups.

Queries all workspaces and active audiences from the targeting API,
compares targetingOutlookDays, audienceSize, audienceSizePercentage,
and exclude_visitors against documented defaults.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

from src.paths import (
    SCRIPT_DIR,
    SIGNAL_CONFIGURATION_DATA_DIR as DATA_DIR,
    SIGNAL_CONFIGURATION_LOGS_DIR as LOGS_DIR,
)

MANUAL_COMMENTS_PATH = DATA_DIR / "manually_change_comments.json"
CUSTOMER_SPECS_PATH = (
    Path(__file__).resolve().parents[3]
    / "innkeepr-analytics"
    / "configs"
    / "customer_specifications.yaml"
)

DEFAULT_AUDIENCE_SIZE = 150_000
DEFAULT_AUDIENCE_PERC_GOOGLE = 0.5
DEFAULT_AUDIENCE_PERC_TIKTOK = 0.5
DEFAULT_AUDIENCE_PERC_EXCLUSION = 0.5
DEFAULT_AUDIENCE_PERC_RETARGETING = 0.5
DEFAULT_CONVERSION_LAG_SEED_2 = 180
DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING = 180
DEFAULT_TARGETING_OUTLOOK_DAYS = 30
DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION = {
    "facebook": 90,
    "googleAnalytics": 30,
    "googleAdwords": 30,
    "tiktok": 30,
    "criteo": 30,
}
EXCLUDED_SOURCES = {"googleAdwords"}
DEFAULT_CONFIGS_DOC_URL = (
    "https://coda.io/d/_dLhoSqeSHRj/Signal-Update-Filters-IN-PROGRESS_su9pf4Ao"
    "#Signal-Default-Setups_tuUNl2Rm"
)

# Special exclusion overrides (name-dependent, from EN-3327 spec).
EXCLUSION_NAME_OVERRIDES = {
    "Innkeepr - 30d Visitors - Exclusion": {
        "targetingOutlookDays": 30,
        "exclude_visitors": None,
        "audienceSizePerc": 0.1,
    },
    "Innkeepr - 30-90d Visitors - Exclusion": {
        "targetingOutlookDays": 90,
        "exclude_visitors": 30,
        "audienceSizePerc": 0.3,
    },
    "Innkeepr - 90-180d Visitors - Exclusion": {
        "targetingOutlookDays": 180,
        "exclude_visitors": 90,
        "audienceSizePerc": 0.5,
    },
}

# Additional exclusion defaults inferred from Signal Default Setups patterns.
EXCLUSION_PATTERN_DEFAULTS = [
    (r"30d Visitor", {"targetingOutlookDays": 30, "exclude_visitors": None, "audienceSizePerc": 0.1}),
    (r"30d Visitors", {"targetingOutlookDays": 30, "exclude_visitors": None, "audienceSizePerc": 0.1}),
    (r"30-90d Visitors", {"targetingOutlookDays": 90, "exclude_visitors": 30, "audienceSizePerc": 0.3}),
    (r"90-180d Visitors", {"targetingOutlookDays": 180, "exclude_visitors": 90, "audienceSizePerc": 0.5}),
    (r"360d Purchaser", {"targetingOutlookDays": 180, "exclude_visitors": None}),
    (r"Low AOV", {"targetingOutlookDays": 180, "exclude_visitors": None}),
    (r"90d Brand", {"targetingOutlookDays": 90, "exclude_visitors": None}),
    (r"Brand - Exclusion", {"targetingOutlookDays": 90, "exclude_visitors": None}),
    (r"General - Exclusion", {"targetingOutlookDays": 180, "exclude_visitors": None}),
    (r"Exclusion - Standard", {"targetingOutlookDays": 180, "exclude_visitors": None}),
    (r"Exclusion - Bestandskunden", {"targetingOutlookDays": 180, "exclude_visitors": None}),
    (r"Exclusion - 365d", {"targetingOutlookDays": 365, "exclude_visitors": None}),
    (r"Exclusion - Lifetime", {"targetingOutlookDays": 730, "exclude_visitors": None}),
]

TIER_PATTERN = re.compile(r"t\d+-\d+p", re.IGNORECASE)
TIER_RANGE_PATTERN = re.compile(r"t(\d+)-(\d+)p", re.IGNORECASE)
STANDARD_TIER_RANGES = frozenset({(0, 10), (10, 20), (20, 30)})
RETARGETING_PATTERN = re.compile(r"RTG|Retargeting", re.IGNORECASE)


def sanitize_workspace_name(name: str) -> str:
    name = name.replace(" ", "").replace("-", "").replace("_", "")
    name = name.replace("&", "and").replace("ö", "oe").replace(".", "dot")
    return name.lower()


def load_customer_specifications() -> dict:
    with CUSTOMER_SPECS_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_manual_comments() -> dict:
    if not MANUAL_COMMENTS_PATH.exists():
        return {}
    with MANUAL_COMMENTS_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def get_manual_comment(
    manual_comments: dict, workspace_name: str, audience_id: str
) -> str | None:
    workspace_comments = manual_comments.get(workspace_name, {})
    comment = workspace_comments.get(audience_id)
    if comment is None:
        return None
    return str(comment).strip() or None


def append_manual_comment(
    comment: str, workspace_name: str, audience_id: str, manual_comments: dict
) -> str:
    manual_comment = get_manual_comment(manual_comments, workspace_name, audience_id)
    if not manual_comment:
        return comment
    if comment:
        return f"{comment}; {manual_comment}"
    return manual_comment


def apply_manual_comments_to_table(
    table: pd.DataFrame, manual_comments: dict | None = None
) -> pd.DataFrame:
    if manual_comments is None:
        manual_comments = load_manual_comments()
    if not manual_comments or table.empty:
        return table

    updated_comments = []
    for _, row in table.iterrows():
        existing = row["comment"] if pd.notna(row["comment"]) else ""
        updated_comments.append(
            append_manual_comment(
                existing,
                row["workspace.name"],
                row["audience.id"],
                manual_comments,
            )
        )
    table = table.copy()
    table["comment"] = updated_comments
    return table


def api_post(endpoint: str, token: str, payload: dict) -> list:
    response = requests.post(
        endpoint,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("messages"):
        message = body["messages"][0]
        if message.get("type") in {"exception", "error"}:
            raise RuntimeError(json.dumps(body))
    return body.get("data", [])


def query_all_pages(endpoint: str, token: str, workspace_id: str, content: dict) -> list:
    page = 1
    results: list = []
    while True:
        payload = {
            "content": content,
            "context": {"workspaceId": workspace_id},
            "pagination": {"page": page},
        }
        response = requests.post(
            endpoint,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        body = response.json()
        results.extend(body.get("data", []))
        next_page = body.get("pagination", {}).get("next")
        if not next_page:
            break
        page = next_page
    return results


def get_exclude_visitors(
    customer_specs: dict, workspace_name: str, audience_id: str
) -> int | None:
    workspace_key = sanitize_workspace_name(workspace_name)
    workspace_specs = customer_specs.get(workspace_key, {})
    audience_specs = workspace_specs.get(audience_id, {})
    return audience_specs.get("exclude_visitors")


def is_retargeting_signal(audience_name: str) -> bool:
    return bool(RETARGETING_PATTERN.search(audience_name))


def is_retargeting_audience(audience_name: str, audience_type: str) -> bool:
    return audience_type == "retargeting" or is_retargeting_signal(audience_name)


def normalize_exclusion_overrides(overrides: dict) -> dict:
    normalized = dict(overrides)
    if "audienceSizePerc" in normalized:
        normalized["audienceSizePercentage"] = normalized.pop("audienceSizePerc")
    return normalized


def build_exclusion_defaults(overrides: dict | None = None) -> dict:
    defaults = {
        "audienceSize": None,
        "exclude_visitors": None,
        "targetingOutlookDays": None,
    }
    if overrides:
        normalized = normalize_exclusion_overrides(overrides)
        if "audienceSizePercentage" in normalized:
            defaults["requires_audienceSizePercentage"] = True
        defaults.update(normalized)
    if "audienceSizePercentage" not in defaults:
        defaults["audienceSizePercentage"] = DEFAULT_AUDIENCE_PERC_EXCLUSION
    return defaults


def match_exclusion_defaults(audience_name: str) -> dict | None:
    for exact_name, defaults in EXCLUSION_NAME_OVERRIDES.items():
        if audience_name == exact_name or audience_name.startswith(f"{exact_name} -"):
            return build_exclusion_defaults(defaults)

    for pattern, defaults in EXCLUSION_PATTERN_DEFAULTS:
        if re.search(pattern, audience_name, re.IGNORECASE):
            return build_exclusion_defaults(defaults)
    return None


def is_visitor_exclusion_name(audience_name: str) -> bool:
    return bool(re.search(r"Visitor", audience_name, re.IGNORECASE))


def classify_visitor_exclusion(audience_name: str) -> str | None:
    if not is_visitor_exclusion_name(audience_name):
        return None
    if re.search(r"30-90d Visitors", audience_name, re.IGNORECASE):
        return "30-90d Visitors"
    if re.search(r"90-180d Visitors", audience_name, re.IGNORECASE):
        return "90-180d Visitors"
    if re.search(r"30d Visitors?", audience_name, re.IGNORECASE):
        return "30d Visitors"
    return "Visitors (other)"


VISITOR_EXCLUSION_CATEGORIES = [
    "30d Visitors",
    "30-90d Visitors",
    "90-180d Visitors",
    "Visitors (other)",
]


def extract_visitor_exclusion_suffix(audience_name: str) -> str:
    for base_name in EXCLUSION_NAME_OVERRIDES:
        if "Visitor" not in base_name:
            continue
        if audience_name == base_name:
            return ""
        if audience_name.startswith(f"{base_name} -"):
            return audience_name[len(base_name) :]
    return ""


def matches_visitor_exclusion_config(
    actual_outlook, exclude_visitors, config: dict
) -> bool:
    if not values_equal(actual_outlook, config["targetingOutlookDays"]):
        return False
    expected_exclude = config.get("exclude_visitors")
    if expected_exclude is None:
        return exclude_visitors is None
    return values_equal(exclude_visitors, expected_exclude) or exclude_visitors is None


def suggest_visitor_exclusion_rename(
    audience_name: str,
    audience_type: str,
    actual_outlook,
    exclude_visitors,
    is_correct: bool,
) -> str | None:
    if is_correct or audience_type != "exclusion":
        return None
    if not is_visitor_exclusion_name(audience_name):
        return None

    suffix = extract_visitor_exclusion_suffix(audience_name)
    matching_bases = [
        base_name
        for base_name, config in EXCLUSION_NAME_OVERRIDES.items()
        if "Visitor" in base_name
        and matches_visitor_exclusion_config(actual_outlook, exclude_visitors, config)
    ]
    if not matching_bases:
        return None

    suggested_name = matching_bases[0] + suffix
    if suggested_name == audience_name:
        return None

    return (
        f"Consider renaming to `{suggested_name}` "
        f"(configuration matches `{matching_bases[0]}` setup)"
    )


def validate_with_suggestions(
    actual_outlook,
    actual_size,
    actual_perc,
    actual_exclude_visitors,
    expected: dict,
    audience_name: str,
    audience_type: str = "",
    connection_name: str = "",
) -> tuple[bool, str]:
    label, comment = validate_configuration(
        actual_outlook,
        actual_size,
        actual_perc,
        actual_exclude_visitors,
        expected,
        audience_name,
        audience_type,
        connection_name,
    )
    rename_suggestion = suggest_visitor_exclusion_rename(
        audience_name,
        audience_type,
        actual_outlook,
        actual_exclude_visitors,
        label,
    )
    if rename_suggestion:
        comment = f"{comment}; {rename_suggestion}" if comment else rename_suggestion
    return label, comment


def extract_tier_range(audience_name: str) -> tuple[int, int] | None:
    match = TIER_RANGE_PATTERN.search(audience_name)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def get_custom_tier_seed_percentage(audience_name: str, audience_type: str) -> float | None:
    """Custom seed tier ranges (e.g. t7-15p) expect audienceSizePercentage = max / 100."""
    if audience_type != "seed":
        return None
    tier_range = extract_tier_range(audience_name)
    if tier_range is None or tier_range in STANDARD_TIER_RANGES:
        return None
    return tier_range[1] / 100


def is_standard_tier_signal(audience_name: str, audience_type: str = "") -> bool:
    if not TIER_PATTERN.search(audience_name):
        return False
    return get_custom_tier_seed_percentage(audience_name, audience_type) is None


def is_premium_seed(audience_name: str, audience_type: str) -> bool:
    return audience_type == "seed" and bool(
        re.search(r"Premium", audience_name, re.IGNORECASE)
    )


def is_meta_tier_seed_none_audience_size(audience_name: str, audience_type: str) -> bool:
    """Premium, Growth, and Volume Meta seeds expect audienceSize = None."""
    return audience_type == "seed" and bool(
        re.search(r"Premium|Growth|Volume", audience_name, re.IGNORECASE)
    )


def get_meta_targeting_outlook(audience_name: str, audience_type: str = "") -> int:
    """Meta Premium seeds use 90d; other t0-10p tiers use 30d; Growth/Volume use 90d."""
    if is_premium_seed(audience_name, audience_type):
        return 90
    if re.search(r"t0-10p", audience_name, re.IGNORECASE):
        return 30
    if re.search(r"t10-20p|t20-30p", audience_name, re.IGNORECASE):
        return 90
    return DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION["facebook"]


def is_valid_value_based_percentage(actual_perc) -> bool:
    return actual_perc is None or values_equal(actual_perc, 1)


def is_meta_seed(audience_type: str, connection_name: str) -> bool:
    return audience_type == "seed" and connection_name == "facebook"


def meta_seed_allows_none_audience_size(actual_perc) -> bool:
    return actual_perc is not None


def is_360d_purchaser_exclusion(audience_name: str) -> bool:
    return bool(re.search(r"360d Purchaser", audience_name, re.IGNORECASE))


def is_valid_exclusion_percentage(
    actual_perc,
    audience_name: str = "",
    expected: dict | None = None,
) -> bool:
    expected_perc = (
        expected.get("audienceSizePercentage")
        if expected is not None
        else None
    )
    if expected_perc is not None:
        if values_equal(actual_perc, expected_perc):
            return True
        requires_explicit = (
            expected.get("requires_audienceSizePercentage", False)
            if expected is not None
            else False
        )
        if not requires_explicit and actual_perc is None:
            return True
        if is_360d_purchaser_exclusion(audience_name) and values_equal(actual_perc, 1):
            return True
        return False

    if actual_perc is None or values_equal(actual_perc, DEFAULT_AUDIENCE_PERC_EXCLUSION):
        return True
    return is_360d_purchaser_exclusion(audience_name) and values_equal(actual_perc, 1)


def expected_exclusion_percentage_label(
    audience_name: str = "",
    expected: dict | None = None,
) -> str:
    expected_perc = (
        expected.get("audienceSizePercentage")
        if expected is not None
        else None
    )
    if expected_perc is not None:
        requires_explicit = (
            expected.get("requires_audienceSizePercentage", False)
            if expected is not None
            else False
        )
        label = str(expected_perc)
        if not requires_explicit:
            label = f"None, {expected_perc}"
        if is_360d_purchaser_exclusion(audience_name):
            return f"{label} or 1"
        return label
    if is_360d_purchaser_exclusion(audience_name):
        return "None, 0.5, or 1"
    return "None or 0.5"


def get_platform_defaults(connection_name: str, audience_name: str, audience_type: str) -> dict:
    if audience_type == "value-based":
        targeting_outlook_days = DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION.get(
            connection_name, DEFAULT_TARGETING_OUTLOOK_DAYS
        )
        return {
            "targetingOutlookDays": targeting_outlook_days,
            "audienceSize": None,
            "audienceSizePercentage": None,
            "exclude_visitors": None,
        }

    if audience_type == "exclusion" or "Exclusion" in audience_name:
        exclusion_defaults = match_exclusion_defaults(audience_name)
        if exclusion_defaults:
            return exclusion_defaults
        return build_exclusion_defaults()

    targeting_outlook_days = DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION.get(
        connection_name, DEFAULT_TARGETING_OUTLOOK_DAYS
    )
    is_retargeting = is_retargeting_audience(audience_name, audience_type)
    if is_retargeting:
        targeting_outlook_days = DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING
    audience_size_percentage = (
        DEFAULT_AUDIENCE_PERC_RETARGETING if is_retargeting else None
    )

    if connection_name == "facebook":
        custom_tier_percentage = get_custom_tier_seed_percentage(audience_name, audience_type)
        meta_outlook = (
            DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING
            if is_retargeting
            else get_meta_targeting_outlook(audience_name, audience_type)
        )
        return {
            "targetingOutlookDays": meta_outlook,
            "audienceSize": (
                None
                if is_meta_tier_seed_none_audience_size(audience_name, audience_type)
                else DEFAULT_AUDIENCE_SIZE
            ),
            "audienceSizePercentage": custom_tier_percentage,
            "conversionLag": DEFAULT_CONVERSION_LAG_SEED_2 if "#2" in audience_name else None,
            "exclude_visitors": None,
        }

    if connection_name == "tiktok":
        return {
            "targetingOutlookDays": targeting_outlook_days,
            "audienceSize": None,
            "audienceSizePercentage": (
                audience_size_percentage or DEFAULT_AUDIENCE_PERC_TIKTOK
            ),
            "conversionLag": DEFAULT_CONVERSION_LAG_SEED_2 if "#2" in audience_name else None,
            "exclude_visitors": None,
        }

    if audience_type == "seed" and connection_name == "googleAnalytics":
        return {
            "targetingOutlookDays": targeting_outlook_days,
            "audienceSize": None,
            "audienceSizePercentage": DEFAULT_AUDIENCE_PERC_GOOGLE,
            "conversionLag": DEFAULT_CONVERSION_LAG_SEED_2 if "#2" in audience_name else None,
            "exclude_visitors": None,
        }

    return {
        "targetingOutlookDays": targeting_outlook_days,
        "audienceSize": DEFAULT_AUDIENCE_SIZE,
        "audienceSizePercentage": (
            audience_size_percentage or DEFAULT_AUDIENCE_PERC_GOOGLE
        ),
        "conversionLag": DEFAULT_CONVERSION_LAG_SEED_2 if "#2" in audience_name else None,
        "exclude_visitors": None,
    }


def values_equal(actual, expected) -> bool:
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False
    if isinstance(actual, float) and isinstance(expected, (int, float)):
        return abs(actual - expected) < 1e-9
    return actual == expected


def validate_configuration(
    actual_outlook,
    actual_size,
    actual_perc,
    actual_exclude_visitors,
    expected: dict,
    audience_name: str,
    audience_type: str = "",
    connection_name: str = "",
) -> tuple[bool, str]:
    issues: list[str] = []

    expected_outlook = expected.get("targetingOutlookDays")
    if expected_outlook is not None and not values_equal(actual_outlook, expected_outlook):
        issues.append(
            f"targetingOutlookDays is {actual_outlook}, expected {expected_outlook}"
        )

    expected_exclude = expected.get("exclude_visitors")
    if expected_exclude is not None and not values_equal(actual_exclude_visitors, expected_exclude):
        issues.append(
            f"exclude_visitors is {actual_exclude_visitors}, expected {expected_exclude}"
        )
    elif expected_exclude is None and actual_exclude_visitors is not None:
        if "Exclusion" not in audience_name:
            issues.append(
                f"exclude_visitors is {actual_exclude_visitors}, expected None"
            )

    is_exclusion = audience_type == "exclusion" or "Exclusion" in audience_name
    custom_tier_percentage = get_custom_tier_seed_percentage(audience_name, audience_type)
    is_standard_tier = is_standard_tier_signal(audience_name, audience_type)
    is_ga_seed = audience_type == "seed" and connection_name == "googleAnalytics"
    is_meta_seed_audience = is_meta_seed(audience_type, connection_name)
    tier_seed_none_size = is_meta_tier_seed_none_audience_size(
        audience_name, audience_type
    )

    if "audienceSize" in expected and not is_exclusion and audience_type != "value-based":
        if not is_ga_seed and not (
            is_meta_seed_audience
            and meta_seed_allows_none_audience_size(actual_perc)
            and not tier_seed_none_size
        ):
            expected_size = expected["audienceSize"]
            if expected_size is not None and not values_equal(actual_size, expected_size):
                issues.append(f"audienceSize is {actual_size}, expected {expected_size}")
            if expected_size is None and actual_size is not None:
                issues.append(f"audienceSize is {actual_size}, expected None")

    if "audienceSizePercentage" in expected and not is_standard_tier:
        if audience_type == "value-based":
            if not is_valid_value_based_percentage(actual_perc):
                issues.append(
                    f"audienceSizePercentage is {actual_perc}, expected 1 or None"
                )
        elif custom_tier_percentage is not None:
            if not values_equal(actual_perc, custom_tier_percentage):
                issues.append(
                    f"audienceSizePercentage is {actual_perc}, expected "
                    f"{custom_tier_percentage} (max tier value)"
                )
        elif is_exclusion:
            if not is_valid_exclusion_percentage(actual_perc, audience_name, expected):
                issues.append(
                    f"audienceSizePercentage is {actual_perc}, expected "
                    f"{expected_exclusion_percentage_label(audience_name, expected)}"
                )
        elif is_ga_seed:
            if actual_perc is None:
                issues.append("audienceSizePercentage is None, expected to be defined (0.5)")
            elif not values_equal(actual_perc, DEFAULT_AUDIENCE_PERC_GOOGLE):
                issues.append(
                    f"audienceSizePercentage is {actual_perc}, expected {DEFAULT_AUDIENCE_PERC_GOOGLE}"
                )
        elif expected.get("audienceSizePercentage") is not None:
            expected_perc = expected["audienceSizePercentage"]
            if not values_equal(actual_perc, expected_perc):
                issues.append(
                    f"audienceSizePercentage is {actual_perc}, expected {expected_perc}"
                )

    return (len(issues) == 0, "; ".join(issues))


def fetch_model(api_url: str, token: str, workspace_id: str, model_id: str | None) -> dict:
    if not model_id:
        return {}
    models = api_post(
        f"{api_url}/api/models/query",
        token,
        {"content": {"id": model_id}, "context": {"workspaceId": workspace_id}},
    )
    return models[0] if models else {}


def get_treatments_count(config: dict) -> int:
    treatments = config.get("treatments")
    if not treatments:
        return 0
    return len(treatments)


def build_audit_table(
    api_url: str,
    token: str,
    customer_specs: dict,
    manual_comments: dict | None = None,
) -> pd.DataFrame:
    if manual_comments is None:
        manual_comments = load_manual_comments()
    rows: list[dict] = []
    workspaces = api_post(f"{api_url}/api/core/workspaces/query", token, {"content": {}})

    for workspace in workspaces:
        workspace_id = workspace["id"]
        workspace_name = workspace["name"]

        connections = api_post(
            f"{api_url}/api/connections/query",
            token,
            {"content": {}, "context": {"workspaceId": workspace_id}},
        )
        connection_names = {
            conn.get("_id") or conn.get("id"): conn["name"] for conn in connections
        }

        audiences = query_all_pages(
            f"{api_url}/api/audiences/query",
            token,
            workspace_id,
            {},
        )

        for audience in audiences:
            if audience.get("status") != "active":
                continue

            audience_id = audience["id"]
            audience_name = audience["name"]
            audience_type = audience.get("type")
            connection_id = audience.get("connection") or audience.get("source")
            connection_name = connection_names.get(connection_id, connection_id)
            if connection_name in EXCLUDED_SOURCES:
                continue

            config = audience.get("config", {})
            targeting_outlook_days = config.get("targetingOutlookDays")
            model = fetch_model(api_url, token, workspace_id, config.get("model"))
            model_type = model.get("type")
            audience_size = model.get("audienceSize")
            audience_size_percentage = model.get("audienceSizePercentage")
            treatments_count = get_treatments_count(config)
            exclude_visitors = get_exclude_visitors(
                customer_specs, workspace_name, audience_id
            )

            expected = get_platform_defaults(
                connection_name, audience_name, audience.get("type", "")
            )
            label, comment = validate_with_suggestions(
                targeting_outlook_days,
                audience_size,
                audience_size_percentage,
                exclude_visitors,
                expected,
                audience_name,
                audience_type or "",
                connection_name,
            )
            comment = append_manual_comment(
                comment, workspace_name, audience_id, manual_comments
            )

            if audience_type == "exclusion":
                audience_size = None

            rows.append(
                {
                    "workspace.name": workspace_name,
                    "audience.id": audience_id,
                    "audience.name": audience_name,
                    "audience.type": audience_type,
                    "audience.source": connection_name,
                    "model.type": model_type,
                    "audience.treatments.count": treatments_count,
                    "audience.targetingOutlookDays": targeting_outlook_days,
                    "audience.audienceSizePercentage": audience_size_percentage,
                    "audience.audienceSize": audience_size,
                    "exclude_visitors": exclude_visitors,
                    "label": label,
                    "comment": comment,
                }
            )

    return pd.DataFrame(rows)


def filter_audit_table(table: pd.DataFrame) -> pd.DataFrame:
    if table.empty or "audience.source" not in table.columns:
        return table
    return table[~table["audience.source"].isin(EXCLUDED_SOURCES)].reset_index(drop=True)


def sort_audit_table(table: pd.DataFrame) -> pd.DataFrame:
    return filter_audit_table(table).sort_values(
        by=["audience.type", "audience.name", "workspace.name"],
        na_position="last",
    ).reset_index(drop=True)


def normalize_audit_value(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def get_row_context(row: pd.Series) -> dict:
    audience_type = row["audience.type"] if pd.notna(row["audience.type"]) else ""
    audience_name = row["audience.name"]
    connection_name = row["audience.source"]
    return {
        "audience_type": audience_type,
        "audience_name": audience_name,
        "connection_name": connection_name,
        "is_exclusion": audience_type == "exclusion" or "Exclusion" in audience_name,
        "is_tier_signal": bool(TIER_PATTERN.search(audience_name)),
        "custom_tier_percentage": get_custom_tier_seed_percentage(
            audience_name, audience_type
        ),
        "is_standard_tier": is_standard_tier_signal(audience_name, audience_type),
        "is_ga_seed": audience_type == "seed" and connection_name == "googleAnalytics",
        "is_meta_seed": is_meta_seed(audience_type, connection_name),
        "is_tier_seed_none_audience_size": is_meta_tier_seed_none_audience_size(
            audience_name, audience_type
        ),
        "expected": get_platform_defaults(connection_name, audience_name, audience_type),
        "actuals": {
            "targetingOutlookDays": normalize_audit_value(row["audience.targetingOutlookDays"]),
            "audienceSizePercentage": normalize_audit_value(
                row["audience.audienceSizePercentage"]
            ),
            "audienceSize": normalize_audit_value(row["audience.audienceSize"]),
            "exclude_visitors": normalize_audit_value(row["exclude_visitors"]),
        },
    }


def field_is_checked(context: dict, field: str) -> bool:
    audience_type = context["audience_type"]
    if field == "audienceSize":
        if context["is_exclusion"] or audience_type == "value-based" or context["is_ga_seed"]:
            return False
        if (
            context["is_meta_seed"]
            and meta_seed_allows_none_audience_size(context["actuals"]["audienceSizePercentage"])
            and not context["is_tier_seed_none_audience_size"]
        ):
            return False
        return True
    if field == "audienceSizePercentage":
        return not context["is_standard_tier"]
    return True


def field_matches_default(context: dict, field: str) -> bool:
    actual = context["actuals"][field]
    expected = context["expected"].get(field)
    audience_type = context["audience_type"]

    if field == "targetingOutlookDays":
        return expected is None or values_equal(actual, expected)

    if field == "exclude_visitors":
        if expected is not None:
            return values_equal(actual, expected)
        return actual is None or context["is_exclusion"]

    if field == "audienceSize":
        if (
            context["is_meta_seed"]
            and meta_seed_allows_none_audience_size(context["actuals"]["audienceSizePercentage"])
            and not context["is_tier_seed_none_audience_size"]
        ):
            return True
        if expected is None:
            return actual is None
        return values_equal(actual, expected)

    if field == "audienceSizePercentage":
        if audience_type == "value-based":
            return is_valid_value_based_percentage(actual)
        if context["custom_tier_percentage"] is not None:
            return values_equal(actual, context["custom_tier_percentage"])
        if context["is_exclusion"]:
            return is_valid_exclusion_percentage(
                actual, context["audience_name"], context["expected"]
            )
        if context["is_ga_seed"]:
            return actual is not None and values_equal(actual, DEFAULT_AUDIENCE_PERC_GOOGLE)
        if expected is None:
            return actual is None
        return values_equal(actual, expected)

    return True


def analyze_type_default_suggestions(type_table: pd.DataFrame) -> list[str]:
    fields = [
        "targetingOutlookDays",
        "audienceSizePercentage",
        "audienceSize",
        "exclude_visitors",
    ]
    suggestions: list[str] = []

    for field in fields:
        checked: list[tuple] = []
        for _, row in type_table.iterrows():
            context = get_row_context(row)
            if not field_is_checked(context, field):
                continue
            checked.append(
                (
                    context["actuals"][field],
                    context["expected"].get(field),
                    field_matches_default(context, field),
                )
            )

        if not checked:
            continue

        mismatch_count = sum(1 for _, _, matches in checked if not matches)
        if mismatch_count <= len(checked) / 2:
            continue

        actual_counter = Counter(actual for actual, _, _ in checked)
        mode_actual, mode_count = actual_counter.most_common(1)[0]
        expected_counter = Counter(
            expected for _, expected, _ in checked if expected is not None
        )
        if expected_counter:
            typical_expected = expected_counter.most_common(1)[0][0]
        else:
            typical_expected = "varies"

        suggestions.append(
            f"- **`{field}`**: {mode_count}/{len(checked)} signals use "
            f"`{format_markdown_value(mode_actual)}` vs current default "
            f"`{format_markdown_value(typical_expected)}` "
            f"({mismatch_count}/{len(checked)} mismatches). "
            f"**Suggest new default: `{format_markdown_value(mode_actual)}`**"
        )

    return suggestions


def format_markdown_value(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    if isinstance(value, bool):
        text = str(value)
    elif isinstance(value, float) and value.is_integer():
        text = str(int(value))
    else:
        text = str(value)
    return text.replace("|", "\\|")


SOURCE_OVERVIEW_HEADERS = [
    "Audience type",
    "`targetingOutlookDays`",
    "`audienceSize`",
    "`audienceSizePercentage`",
    "`conversionLag`",
]

EXCLUSION_OVERVIEW_HEADERS = [
    "Signal / pattern",
    "`targetingOutlookDays`",
    "`audienceSize`",
    "`audienceSizePercentage`",
    "`exclude_visitors`",
]


def _source_overview_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _build_meta_defaults_section() -> list[str]:
    return [
        "### Meta (`facebook`)",
        "",
        *_source_overview_table(
            SOURCE_OVERVIEW_HEADERS,
            [
                ["Default", "90", f"{DEFAULT_AUDIENCE_SIZE:,}", "-", f"{DEFAULT_CONVERSION_LAG_SEED_2} (Seed #2)"],
                ["Seed – Premium (`t0-10p`)", "90", "-", "0 - 10", "-"],
                ["Seed – Growth (`t10-20p`)", "90", "-", "10 - 20", "-"],
                ["Seed – Volume (`t20-30p`)", "90", "-", "20 - 30", "-"],
                [
                    "Seed – Custom tier (`t7-15p`, …)",
                    "90",
                    f"{DEFAULT_AUDIENCE_SIZE:,}",
                    "max tier value / 100",
                    "-",
                ],
                ["Value-based", "90", "-", "1 or `-`", "-"],
                [
                    "Retargeting",
                    str(DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING),
                    f"{DEFAULT_AUDIENCE_SIZE:,}",
                    str(DEFAULT_AUDIENCE_PERC_RETARGETING),
                    "-",
                ],
            ],
        ),
        "",
    ]


def _build_google_analytics_defaults_section() -> list[str]:
    outlook_days = str(DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION["googleAnalytics"])
    return [
        "### Google Analytics (`googleAnalytics`)",
        "",
        *_source_overview_table(
            SOURCE_OVERVIEW_HEADERS,
            [
                [
                    "Default",
                    outlook_days,
                    f"{DEFAULT_AUDIENCE_SIZE:,}",
                    str(DEFAULT_AUDIENCE_PERC_GOOGLE),
                    f"{DEFAULT_CONVERSION_LAG_SEED_2} (Seed #2)",
                ],
                [
                    "Retargeting",
                    str(DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING),
                    f"{DEFAULT_AUDIENCE_SIZE:,}",
                    str(DEFAULT_AUDIENCE_PERC_RETARGETING),
                    "-",
                ],
            ],
        ),
        "",
    ]


def _build_criteo_defaults_section() -> list[str]:
    outlook_days = str(DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION["criteo"])
    return [
        "### Criteo (`criteo`)",
        "",
        *_source_overview_table(
            SOURCE_OVERVIEW_HEADERS,
            [
                [
                    "Default",
                    outlook_days,
                    f"{DEFAULT_AUDIENCE_SIZE:,}",
                    str(DEFAULT_AUDIENCE_PERC_GOOGLE),
                    f"{DEFAULT_CONVERSION_LAG_SEED_2} (Seed #2)",
                ],
                [
                    "Retargeting",
                    str(DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING),
                    f"{DEFAULT_AUDIENCE_SIZE:,}",
                    str(DEFAULT_AUDIENCE_PERC_RETARGETING),
                    "-",
                ],
            ],
        ),
        "",
    ]


def _build_tiktok_defaults_section() -> list[str]:
    outlook_days = str(DEFAULT_TARGETING_OUTLOOK_DAYS_BY_CONNECTION["tiktok"])
    return [
        "### TikTok (`tiktok`)",
        "",
        *_source_overview_table(
            SOURCE_OVERVIEW_HEADERS,
            [
                [
                    "Default",
                    outlook_days,
                    "`-`",
                    str(DEFAULT_AUDIENCE_PERC_TIKTOK),
                    f"{DEFAULT_CONVERSION_LAG_SEED_2} (Seed #2)",
                ],
                [
                    "Retargeting",
                    str(DEFAULT_TARGETING_OUTLOOK_DAYS_RETARGETING),
                    "`-`",
                    str(DEFAULT_AUDIENCE_PERC_RETARGETING),
                    "-",
                ],
            ],
        ),
        "",
    ]


def _build_exclusions_defaults_section() -> list[str]:
    default_percentage = (
        f"{DEFAULT_AUDIENCE_PERC_EXCLUSION} or `-` (innkeepr-analytics uses 0.5 when unset)"
    )
    rows = [
        ["Default", "per signal / pattern", "`-`", default_percentage, "from `customer_specifications.yaml`"],
    ]
    for name, defaults in EXCLUSION_NAME_OVERRIDES.items():
        normalized = normalize_exclusion_overrides(defaults)
        rows.append(
            [
                f"`{name}`",
                str(normalized["targetingOutlookDays"]),
                "-",
                format_markdown_value(normalized.get("audienceSizePercentage")),
                format_markdown_value(normalized["exclude_visitors"]),
            ]
        )
    for pattern, defaults in EXCLUSION_PATTERN_DEFAULTS:
        normalized = normalize_exclusion_overrides(defaults)
        rows.append(
            [
                f"`{pattern}`",
                format_markdown_value(normalized["targetingOutlookDays"]),
                "-",
                format_markdown_value(normalized.get("audienceSizePercentage")),
                format_markdown_value(normalized["exclude_visitors"]),
            ]
        )

    return [
        "### Exclusions (all platforms)",
        "",
        *_source_overview_table(EXCLUSION_OVERVIEW_HEADERS, rows),
        "",
        "Visitor exclusions (`30d`, `30-90d`, `90-180d`) require an explicit "
        "`audienceSizePercentage` (`0.1`, `0.3`, `0.5`); `None` is not allowed.",
        "",
        "`360d Purchaser` exclusions also allow `audienceSizePercentage` = `1`.",
        "",
    ]


def build_defaults_markdown() -> str:
    lines = [
        "## Default Configurations",
        "",
        f"Reference: [Signal Default Setups]({DEFAULT_CONFIGS_DOC_URL})",
        "",
        *_build_meta_defaults_section(),
        *_build_google_analytics_defaults_section(),
        *_build_criteo_defaults_section(),
        *_build_tiktok_defaults_section(),
        *_build_exclusions_defaults_section(),
    ]
    return "\n".join(lines)


def format_result_comment(row: pd.Series) -> str:
    comment = row["comment"]
    if pd.notna(comment) and str(comment).strip():
        return format_markdown_value(comment)
    if bool(row["label"]):
        return "Configuration matches defaults"
    return ""


def build_audience_table_markdown(source_table: pd.DataFrame) -> list[str]:
    lines = [
        "| Status | Workspace | `audience.id` | Audience | Source | `model.type` | "
        "`audience.treatments.count` | Outlook | Size % | Size | "
        "`exclude_visitors` | Result |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for _, row in source_table.iterrows():
        is_correct = bool(row["label"])
        status = "✅" if is_correct else "❌"
        result = format_result_comment(row)
        lines.append(
            "| {status} | {workspace} | {audience_id} | {name} | {source} | {model_type} | "
            "{treatments_count} | {outlook} | {perc} | {size} | {exclude} | {result} |".format(
                status=status,
                workspace=format_markdown_value(row["workspace.name"]),
                audience_id=format_markdown_value(row["audience.id"]),
                name=format_markdown_value(row["audience.name"]),
                source=format_markdown_value(row["audience.source"]),
                model_type=format_markdown_value(row.get("model.type")),
                treatments_count=format_markdown_value(row.get("audience.treatments.count")),
                outlook=format_markdown_value(row["audience.targetingOutlookDays"]),
                perc=format_markdown_value(row["audience.audienceSizePercentage"]),
                size=format_markdown_value(row["audience.audienceSize"]),
                exclude=format_markdown_value(row["exclude_visitors"]),
                result=result,
            )
        )

    return lines


def format_audience_count_label(count: int) -> str:
    noun = "audience" if count == 1 else "audiences"
    return f"{count} {noun}"


def build_visitor_exclusion_summary_lines(type_table: pd.DataFrame) -> list[str]:
    visitor_table = type_table[
        type_table["audience.name"].apply(is_visitor_exclusion_name)
    ]
    if visitor_table.empty:
        return []

    correct_count = int(visitor_table["label"].sum())
    incorrect_count = len(visitor_table) - correct_count
    lines = [
        "### Visitors",
        "",
        (
            f"**{len(visitor_table)}** visitor exclusion audiences "
            f"({correct_count} correct, {incorrect_count} incorrect)."
        ),
        "",
    ]

    for category in VISITOR_EXCLUSION_CATEGORIES:
        category_table = visitor_table[
            visitor_table["audience.name"].apply(
                lambda name: classify_visitor_exclusion(name) == category
            )
        ]
        if category_table.empty:
            continue
        category_correct = int(category_table["label"].sum())
        category_incorrect = len(category_table) - category_correct
        lines.append(
            f"- **{category}:** {format_audience_count_label(len(category_table))} "
            f"({category_correct} correct, {category_incorrect} incorrect)"
        )

    lines.append("")
    return lines


def build_type_section_markdown(audience_type: str, type_table: pd.DataFrame) -> str:
    type_label = str(audience_type) if pd.notna(audience_type) else "unknown"
    correct_count = int(type_table["label"].sum())
    incorrect_count = len(type_table) - correct_count

    lines = [
        f"## {type_label}",
        "",
        f"**{len(type_table)}** active audiences "
        f"({correct_count} correct, {incorrect_count} incorrect).",
        "",
    ]

    if type_label == "exclusion":
        lines.extend(build_visitor_exclusion_summary_lines(type_table))

    suggestions = analyze_type_default_suggestions(type_table)
    if suggestions:
        lines.extend(
            [
                "### Suggested default changes",
                "",
                (
                    "A majority of signals in this type differ from the current defaults "
                    "for the fields below:"
                ),
                "",
                *suggestions,
                "",
            ]
        )

    for source, source_table in type_table.groupby("audience.source", dropna=False, sort=True):
        source_label = str(source) if pd.notna(source) else "unknown"
        source_table = source_table.sort_values(
            by=["workspace.name", "audience.name"],
            na_position="last",
        )
        source_correct = int(source_table["label"].sum())
        source_incorrect = len(source_table) - source_correct

        lines.extend(
            [
                f"### {source_label}",
                "",
                (
                    f"**{len(source_table)}** audiences "
                    f"({source_correct} correct, {source_incorrect} incorrect)."
                ),
                "",
                *build_audience_table_markdown(source_table),
                "",
            ]
        )

    return "\n".join(lines)


def build_audit_markdown(table: pd.DataFrame) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    correct_count = int(table["label"].sum())
    incorrect_count = len(table) - correct_count

    lines = [
        "# Signal Configuration Audit",
        "",
        f"Generated: {generated_at}",
        "",
        "## Summary",
        "",
        f"- **Total active audiences:** {len(table)}",
        f"- **Correct configurations:** {correct_count}",
        f"- **Incorrect configurations:** {incorrect_count}",
        "",
        build_defaults_markdown(),
    ]

    for audience_type, type_table in table.groupby("audience.type", dropna=False, sort=True):
        lines.append(build_type_section_markdown(audience_type, type_table))

    return "\n".join(lines)


def save_audit_markdown(table: pd.DataFrame, output_dir: Path) -> Path:
    markdown_path = output_dir / "signal_configuration_audit.md"
    markdown_path.write_text(build_audit_markdown(table), encoding="utf-8")
    return markdown_path


def save_audit_tables(table: pd.DataFrame, output_dir: Path) -> list[Path]:
    saved_paths: list[Path] = []

    combined_path = output_dir / "signal_configuration_audit.csv"
    table.to_csv(combined_path, index=False)
    saved_paths.append(combined_path)

    for audience_type, type_table in table.groupby("audience.type", dropna=False):
        type_label = str(audience_type) if pd.notna(audience_type) else "unknown"
        type_path = output_dir / f"signal_configuration_audit_{type_label}.csv"
        type_table.to_csv(type_path, index=False)
        saved_paths.append(type_path)

    return saved_paths


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    load_dotenv(SCRIPT_DIR / ".env")

    api_url = os.environ["TARGETING_URL"].rstrip("/")
    token = os.environ["API_SERVICE_TOKEN"]
    customer_specs = load_customer_specifications()
    manual_comments = load_manual_comments()

    logging.info("Querying workspaces and active audiences...")
    table = sort_audit_table(
        build_audit_table(api_url, token, customer_specs, manual_comments)
    )

    saved_paths = save_audit_tables(table, DATA_DIR)
    markdown_path = save_audit_markdown(table, DATA_DIR)
    saved_paths.append(markdown_path)

    incorrect = table[table["label"] == False]  # noqa: E712
    logging.info("Total active audiences: %s", len(table))
    logging.info("Correct configurations: %s", len(table[table["label"] == True]))  # noqa: E712
    logging.info("Incorrect configurations: %s", len(incorrect))
    for path in saved_paths:
        logging.info("Saved audit table to %s", path)

    pd.set_option("display.max_colwidth", 60)
    pd.set_option("display.width", 240)
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
