"""
Databricks Resources Module: List compute resources like SQL Warehouses.
"""

import os
from typing import Dict

import requests
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from context import get_request_credentials

load_dotenv()


class DatabricksResourcesClient:
    """Client to interact with the Databricks Resources API (e.g., SQL Warehouses)."""

    def __init__(self):
        creds = get_request_credentials()
        if creds:
            self.host = creds.host
            self.token = creds.token
        else:
            self.host = os.getenv("DATABRICKS_HOST")
            self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required"
            )
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        # Note: This API version is 2.0, different from Unity Catalog's 2.1
        self.base_url = f"{self.host}/api/2.0/sql/warehouses"

    def list_warehouses(self) -> Dict:
        """List all available SQL Warehouses in the workspace."""
        response = requests.get(self.base_url, headers=self.headers)
        response.raise_for_status()
        return response.json()


def get_resources_client():
    """Creates a new DatabricksResourcesClient with credentials from the current request."""
    return DatabricksResourcesClient()


def mcp_tools(mcp: FastMCP):
    """Registers resource-related tools with the MCP server."""

    @mcp.tool()
    def list_sql_warehouses() -> Dict:
        """Lists all available SQL Warehouses to find a 'warehouse_id' for running queries."""
        try:
            return get_resources_client().list_warehouses()
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to list SQL Warehouses: {e}")

    return mcp
