"""
Databricks Resources Module: List compute resources like SQL Warehouses.
"""
import os
from typing import Dict

import requests
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()


class DatabricksResourcesClient:
    """Client to interact with the Databricks Resources API (e.g., SQL Warehouses)."""

    def __init__(self):
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


# --- Lazy Initialization of the client ---
_resources_client_instance = None

def get_resources_client():
    """Lazily initializes and returns the DatabricksResourcesClient."""
    global _resources_client_instance
    if _resources_client_instance is None:
        _resources_client_instance = DatabricksResourcesClient()
    return _resources_client_instance


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
