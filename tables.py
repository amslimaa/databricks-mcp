"""
Databricks Tables Module: CRUD operations for Unity Catalog tables.
"""

from typing import List, Dict, Optional
import requests
import os
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()

class DatabricksTableClient:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.base_url = f"{self.host}/api/2.1/unity-catalog/tables"

    def list_tables(self, catalog_name: str, schema_name: str) -> Dict:
        """List all tables in a specific schema."""
        params = {"catalog_name": catalog_name, "schema_name": schema_name}
        response = requests.get(self.base_url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_table(self, full_name: str) -> Dict:
        """Get information about a specific table."""
        url = f"{self.base_url}/{full_name}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def create_table(self, table_info: Dict) -> Dict:
        """Create a new table in a schema."""
        response = requests.post(self.base_url, headers=self.headers, json=table_info)
        response.raise_for_status()
        return response.json()

    def update_table(self, full_name: str, updates: Dict) -> Dict:
        """Update an existing table."""
        url = f"{self.base_url}/{full_name}"
        response = requests.patch(url, headers=self.headers, json=updates)
        response.raise_for_status()
        return response.json()

    def delete_table(self, full_name: str) -> Dict:
        """Delete a table."""
        url = f"{self.base_url}/{full_name}"
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()
        return {"status": "success", "message": f"Table {full_name} deleted."}

# --- Lazy Initialization of the client ---
_table_client_instance = None

def get_table_client():
    """Lazily initializes and returns the DatabricksTableClient."""
    global _table_client_instance
    if _table_client_instance is None:
        _table_client_instance = DatabricksTableClient()
    return _table_client_instance

def mcp_tools(mcp: FastMCP):
    """Registers table-related tools with the MCP server."""

    @mcp.tool()
    def list_tables(catalog_name: str, schema_name: str) -> Dict:
        """
        List all tables in a specific schema.
        Args:
            catalog_name (str): The name of the catalog.
            schema_name (str): The name of the schema.
        Returns:
            Dict: Response from Databricks API, including 'tables'.
        """
        try:
            return get_table_client().list_tables(catalog_name, schema_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to list tables: {e}")

    @mcp.tool()
    def create_table(table_info: Dict) -> Dict:
        """
        Create a new table in a schema.
        Args:
            table_info (Dict): A dictionary containing the table information.
        """
        try:
            return get_table_client().create_table(table_info)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to create table: {e}")

    @mcp.tool()
    def update_table(full_name: str, updates: Dict) -> Dict:
        """
        Update an existing table.
        Args:
            full_name (str): The full name of the table (e.g., 'catalog_name.schema_name.table_name').
            updates (Dict): A dictionary containing the updates to the table.
        """
        try:
            return get_table_client().update_table(full_name, updates)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to update table: {e}")

    @mcp.tool()
    def delete_table(full_name: str) -> Dict:
        """
        Delete a table.
        Args:
            full_name (str): The full name of the table (e.g., 'catalog_name.schema_name.table_name').
        """
        try:
            return get_table_client().delete_table(full_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to delete table: {e}")

    @mcp.resource("table://{catalog_name}.{schema_name}.{table_name}")
    def get_table_info(catalog_name: str, schema_name: str, table_name: str) -> Dict:
        """Get information about a specific table."""
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            return get_table_client().get_table(full_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to get table info: {e}")

    return mcp
