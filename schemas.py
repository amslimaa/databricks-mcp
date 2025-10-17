"""
Databricks Schemas Module: CRUD operations for Unity Catalog schemas.
"""
import os
from typing import Dict, Optional

import requests
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()


class DatabricksSchemaClient:
    """Client to interact with the Databricks Schemas API."""

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
        self.base_url = f"{self.host}/api/2.1/unity-catalog/schemas"

    def list_schemas(self, catalog_name: str) -> Dict:
        """List all schemas in a specific catalog."""
        params = {"catalog_name": catalog_name}
        response = requests.get(self.base_url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def create_schema(
        self, catalog_name: str, name: str, comment: Optional[str] = None, properties: Optional[dict] = None
    ) -> Dict:
        """Create a new schema in a catalog."""
        payload = {"name": name, "catalog_name": catalog_name}
        if comment:
            payload["comment"] = comment
        if properties:
            payload["properties"] = properties
        response = requests.post(self.base_url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def get_schema(self, full_name: str) -> Dict:
        """Get information about a specific schema."""
        url = f"{self.base_url}/{full_name}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def update_schema(
        self, full_name: str, new_name: Optional[str] = None, comment: Optional[str] = None, properties: Optional[dict] = None
    ) -> Dict:
        """Update an existing schema."""
        url = f"{self.base_url}/{full_name}"
        payload = {}
        if new_name:
            payload["new_name"] = new_name
        if comment:
            payload["comment"] = comment
        if properties:
            payload["properties"] = properties
        response = requests.patch(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def delete_schema(self, full_name: str) -> Dict:
        """Delete a schema."""
        url = f"{self.base_url}/{full_name}"
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()
        # A API de delete retorna um corpo vazio em caso de sucesso
        return {"status": "success", "message": f"Schema {full_name} deleted."}


# --- Lazy Initialization of the client ---
_schema_client_instance = None

def get_schema_client():
    """Lazily initializes and returns the DatabricksSchemaClient."""
    global _schema_client_instance
    if _schema_client_instance is None:
        _schema_client_instance = DatabricksSchemaClient()
    return _schema_client_instance


def mcp_tools(mcp: FastMCP):
    """Registers schema-related tools with the MCP server."""

    @mcp.tool()
    def list_schemas(catalog_name: str) -> Dict:
        """
        List all schemas in a specific catalog.
        Args:
            catalog_name (str): The name of the catalog.
        Returns:
            Dict: Response from Databricks API, including 'schemas'.
        """
        try:
            return get_schema_client().list_schemas(catalog_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to list schemas: {e}")

    @mcp.tool()
    def create_schema(
        catalog_name: str, name: str, comment: Optional[str] = None, properties: Optional[dict] = None
    ) -> Dict:
        """
        Create a new schema in a catalog.
        Args:
            catalog_name (str): The name of the parent catalog.
            name (str): The name of the new schema.
            comment (str, optional): A comment for the schema.
            properties (dict, optional): A dictionary of key-value properties.
        """
        try:
            return get_schema_client().create_schema(catalog_name, name, comment, properties)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to create schema: {e}")

    @mcp.tool()
    def update_schema(
        full_name: str, new_name: Optional[str] = None, comment: Optional[str] = None, properties: Optional[dict] = None
    ) -> Dict:
        """
        Update an existing schema.
        Args:
            full_name (str): The full name of the schema (e.g., 'catalog_name.schema_name').
            new_name (str, optional): A new name for the schema.
            comment (str, optional): A new comment for the schema.
            properties (dict, optional): A new set of key-value properties.
        """
        try:
            return get_schema_client().update_schema(full_name, new_name, comment, properties)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to update schema: {e}")

    @mcp.tool()
    def delete_schema(full_name: str) -> Dict:
        """Delete a schema. Args: full_name (str): The full name of the schema (e.g., 'catalog_name.schema_name')."""
        try:
            return get_schema_client().delete_schema(full_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to delete schema: {e}")

    @mcp.resource("schema://{catalog_name}.{schema_name}")
    def get_schema_info(catalog_name: str, schema_name: str) -> Dict:
        """Get information about a specific schema."""
        full_name = f"{catalog_name}.{schema_name}"
        try:
            return get_schema_client().get_schema(full_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to get schema info: {e}")

    return mcp