"""
Match active signals to treatments from signals/usage/query.

Signal matching uses usage.targetings against signal.externalId or signal.name.
Treatment metadata is resolved separately from usage.treatment resource names.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

RELATES_TO_MATCH_FIELDS = (
    "assetGroup",
    "adGroup",
    "adGroupAd",
    "campaign",
    "keyword",
)

TARGETING_IDENTIFIER_KEYS = (
    "name",
    "id",
    "externalId",
    "audienceId",
    "signalId",
    "audienceName",
    "signalName",
)


def get_signal_treatment_ids(signal: dict) -> list[str]:
    treatments = (signal.get("config") or {}).get("treatments")
    if not treatments:
        return []
    if isinstance(treatments, dict):
        ids = treatments.get("ids") or []
        return [str(item) for item in ids if item]
    if isinstance(treatments, (list, tuple, set)):
        return [str(item) for item in treatments if item]
    return []


def get_signal_external_id(signal: dict) -> str | None:
    external_id = signal.get("externalId")
    if external_id is None:
        return None
    text = str(external_id).strip()
    return text or None


def get_signal_connection_id(signal: dict) -> str | None:
    connection = signal.get("connection") or signal.get("source")
    if connection is None:
        return None
    if isinstance(connection, dict):
        platform = connection.get("platform") or {}
        connection_id = platform.get("id") or connection.get("id")
        if connection_id is None:
            return None
        return str(connection_id)
    return str(connection)


def collect_targeting_identifiers(value: Any) -> set[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return set()
    if isinstance(value, (str, int, float, bool)):
        text = str(value).strip()
        return {text} if text else set()
    if isinstance(value, dict):
        collected: set[str] = set()
        for key in TARGETING_IDENTIFIER_KEYS:
            field_value = value.get(key)
            if field_value is not None:
                text = str(field_value).strip()
                if text:
                    collected.add(text)
        for nested in value.values():
            collected.update(collect_targeting_identifiers(nested))
        return collected
    if isinstance(value, (list, tuple, set)):
        collected: set[str] = set()
        for item in value:
            collected.update(collect_targeting_identifiers(item))
        return collected
    text = str(value).strip()
    return {text} if text else set()


def targetings_reference_value(targetings: Any, value: str) -> bool:
    if not value:
        return False
    identifiers = collect_targeting_identifiers(targetings)
    if value in identifiers:
        return True
    return value in str(targetings)


def match_usage_row_to_signal(
    usage_row: dict | pd.Series,
    signal: dict,
) -> str | None:
    targetings = usage_row.get("targetings")
    if targetings is None or (isinstance(targetings, float) and pd.isna(targetings)):
        return None

    external_id = get_signal_external_id(signal)
    if external_id and targetings_reference_value(targetings, external_id):
        return "externalId"

    signal_name = signal.get("name")
    if signal_name and targetings_reference_value(targetings, str(signal_name)):
        return "name"

    return None


def build_treatment_match_keys(treatment: dict) -> set[str]:
    keys: set[str] = set()
    treatment_id = treatment.get("id")
    if treatment_id:
        keys.add(str(treatment_id))

    relates_to = treatment.get("relates_to") or {}
    for field_name in RELATES_TO_MATCH_FIELDS:
        field_value = relates_to.get(field_name)
        if isinstance(field_value, dict):
            resource_name = field_value.get("resourceName")
            if resource_name:
                keys.add(str(resource_name))
            external_id = field_value.get("id")
            if external_id is not None:
                keys.add(str(external_id))
        elif field_value is not None:
            keys.add(str(field_value))

    external_id = treatment.get("externalId")
    if external_id is not None:
        keys.add(str(external_id))
    return keys


def get_treatment_name(treatment: dict) -> str | None:
    name = treatment.get("name")
    return str(name) if name is not None else None


def get_treatment_campaign_name(treatment: dict) -> str | None:
    relates_to = treatment.get("relates_to") or {}
    campaign = relates_to.get("campaign") or {}
    name = campaign.get("name")
    return str(name) if name is not None else None


def build_usage_key_to_treatment(
    treatments_by_id: dict[str, dict],
) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for treatment in treatments_by_id.values():
        for key in build_treatment_match_keys(treatment):
            lookup[key] = treatment
    return lookup


def resolve_treatment_from_usage(
    usage_treatment: Any,
    usage_key_to_treatment: dict[str, dict],
) -> dict | None:
    if usage_treatment is None or (isinstance(usage_treatment, float) and pd.isna(usage_treatment)):
        return None
    return usage_key_to_treatment.get(str(usage_treatment))


def aggregate_signal_matches_by_treatment(
    matches: pd.DataFrame,
    treatments_by_id: dict[str, dict],
    workspace_name: str,
) -> pd.DataFrame:
    columns = [
        "workspace",
        "treatment.id",
        "treatment.name",
        "campaign.name",
        "signal.ids",
        "signal.names",
    ]
    if matches.empty:
        return pd.DataFrame(columns=columns)

    usage_key_to_treatment = build_usage_key_to_treatment(treatments_by_id)
    detail_rows: list[dict] = []

    for _, match in matches.iterrows():
        treatment = resolve_treatment_from_usage(
            match.get("usage.treatment"),
            usage_key_to_treatment,
        )
        if treatment is None:
            continue

        detail_rows.append(
            {
                "workspace": workspace_name,
                "treatment.id": str(treatment.get("id")),
                "treatment.name": get_treatment_name(treatment),
                "campaign.name": get_treatment_campaign_name(treatment),
                "signal.id": match.get("signal.id"),
                "signal.name": match.get("signal.name"),
            }
        )

    if not detail_rows:
        return pd.DataFrame(columns=columns)

    detail = pd.DataFrame(detail_rows)
    grouped = (
        detail.groupby(
            ["workspace", "treatment.id", "treatment.name", "campaign.name"],
            dropna=False,
        )
        .agg(
            signal_ids=(
                "signal.id",
                lambda values: sorted({str(value) for value in values if value}),
            ),
            signal_names=(
                "signal.name",
                lambda values: sorted({str(value) for value in values if value}),
            ),
        )
        .reset_index()
    )
    grouped = grouped.rename(
        columns={
            "signal_ids": "signal.ids",
            "signal_names": "signal.names",
        }
    )
    return (
        grouped[columns]
        .sort_values(
            ["workspace", "campaign.name", "treatment.name"],
            na_position="last",
        )
        .reset_index(drop=True)
    )


def match_signals_to_usage(
    signals: list[dict],
    usage_rows: list[dict],
) -> pd.DataFrame:
    columns = [
        "signal.id",
        "signal.name",
        "signal.externalId",
        "match.method",
        "usage.treatment",
        "usage.connectionId",
        "usage.sourceName",
        "usage.date",
        "usage.spend",
    ]
    if not usage_rows:
        return pd.DataFrame(columns=columns)

    usage_df = pd.json_normalize(usage_rows)
    matched_rows: list[dict] = []

    for signal in signals:
        signal_id = signal.get("id")
        signal_name = signal.get("name")
        external_id = get_signal_external_id(signal)

        for _, usage_row in usage_df.iterrows():
            match_method = match_usage_row_to_signal(usage_row, signal)
            if match_method is None:
                continue

            matched_rows.append(
                {
                    "signal.id": signal_id,
                    "signal.name": signal_name,
                    "signal.externalId": external_id,
                    "match.method": match_method,
                    "usage.treatment": usage_row.get("treatment"),
                    "usage.connectionId": usage_row.get("connectionId"),
                    "usage.sourceName": usage_row.get("sourceName"),
                    "usage.date": usage_row.get("date"),
                    "usage.spend": usage_row.get("spend"),
                    "usage.impressions": usage_row.get("impressions"),
                    "usage.clicks": usage_row.get("clicks"),
                    "usage.conversions": usage_row.get("conversions"),
                }
            )

    if not matched_rows:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(matched_rows)
