"""Databricks SQL warehouse client."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import certifi
import pandas as pd
from databricks import sql

CA_BUNDLE = (
    os.environ.get("SSL_CERT_FILE")
    or os.environ.get("REQUESTS_CA_BUNDLE")
    or certifi.where()
)
os.environ.setdefault("SSL_CERT_FILE", CA_BUNDLE)
os.environ.setdefault("REQUESTS_CA_BUNDLE", CA_BUNDLE)


def get_databricks_workspace_url() -> str:
    host = os.environ["DATABRICKS_HOST"].strip()
    if host.startswith("http://") or host.startswith("https://"):
        return urlparse(host).netloc
    return host.rstrip("/")


def get_databricks_sql_path() -> str:
    http_path = os.environ.get("DATABRICKS_HTTP_PATH") or os.environ.get(
        "DATABRICKS_WAREHOUSE_ID"
    )
    if not http_path:
        raise RuntimeError(
            "DATABRICKS_HTTP_PATH or DATABRICKS_WAREHOUSE_ID must be set"
        )
    return http_path


def get_databricks_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("BEARER_TOKEN")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN or BEARER_TOKEN must be set")
    return token


class DatabricksSQLClient:
    """Client for connecting to Databricks SQL."""

    def __init__(self) -> None:
        self.server_hostname = get_databricks_workspace_url()
        self.http_path = get_databricks_sql_path()
        self.access_token = get_databricks_token()

    def connect(self):
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            query_tags={"account": "DatabricksSQL"},
            _tls_trusted_ca_file=CA_BUNDLE,
            _connect_timeout=60,
            _socket_timeout=300,
        )


def query_databricks_sql(sql_statement: str) -> pd.DataFrame:
    client = DatabricksSQLClient()
    with client.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
            return cursor.fetchall_arrow().to_pandas()
