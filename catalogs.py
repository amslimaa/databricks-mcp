"""
Databricks Catalogs Module: CRUD operations for Unity Catalog catalogs.
"""

from typing import List, Dict, Optional
import requests
import os
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()

class DatabricksCatalogClient:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def list_catalogs(self, page_token: Optional[str] = None) -> Dict:
        """List all catalogs in the Databricks workspace, with pagination support."""
        url = f"{self.host}/api/2.1/unity-catalog/catalogs"
        params = {}
        if page_token:
            params["page_token"] = page_token
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def create_catalog(
        self,
        name: str,
        comment: str = "",
        connection_name: str = "",
        options: Optional[dict] = None,
        properties: Optional[dict] = None,
        provider_name: str = "",
        share_name: str = "",
        storage_root: str = ""
    ) -> Dict:
        url = f"{self.host}/api/2.1/unity-catalog/catalogs"
        payload = {"name": name}
        if comment:
            payload["comment"] = comment
        if connection_name:
            payload["connection_name"] = connection_name
        if options is not None and isinstance(options, dict) and options:
            payload["options"] = options
        if properties is not None and isinstance(properties, dict) and properties:
            payload["properties"] = properties
        if provider_name:
            payload["provider_name"] = provider_name
        if share_name:
            payload["share_name"] = share_name
        if storage_root:
            payload["storage_root"] = storage_root
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def delete_catalog(self, name: str, force: bool = False) -> Dict:
        """Delete a catalog in Databricks. Use force=True to force deletion."""
        url = f"{self.host}/api/2.1/unity-catalog/catalogs/{name}"
        params = {"force": "true"} if force else {}
        response = requests.delete(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_catalog_info(self, catalog_name: str) -> Dict:
        catalogs = self.list_catalogs()
        catalog = next((c for c in catalogs.get("catalogs", []) if c["name"] == catalog_name), None)
        if not catalog:
            raise ValueError(f"Catalog {catalog_name} not found")
        return catalog

# --- Lazy Initialization of the client ---
_catalog_client_instance = None

def get_catalog_client():
    """Lazily initializes and returns the DatabricksCatalogClient."""
    global _catalog_client_instance
    if _catalog_client_instance is None:
        _catalog_client_instance = DatabricksCatalogClient()
    return _catalog_client_instance

# These decorators should be imported and used in main.py

def mcp_tools(mcp):
    @mcp.tool()
    def list_catalogs(page_token: Optional[str] = None) -> Dict:
        """
        List all catalogs in the Databricks workspace, with pagination support.
        Args:
            page_token (str, optional): Token for next page of results
        Returns:
            Dict: Response from Databricks API, including 'catalogs' and 'next_page_token'
        """
        try:
            return get_catalog_client().list_catalogs(page_token=page_token)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to list catalogs: {str(e)}")

    @mcp.tool()
    def create_catalog(
        name: str,
        comment: str = "",
        connection_name: str = "",
        options: Optional[dict] = None,
        properties: Optional[dict] = None,
        provider_name: str = "",
        share_name: str = "",
        storage_root: str = ""
    ) -> Dict:
        """
        Cria um novo catálogo no Databricks Unity Catalog.

        Parâmetros:
            name (str): Nome do catálogo (obrigatório)
            comment (str, opcional): Descrição do catálogo
            connection_name (str, opcional): Nome da conexão externa
            options (dict, opcional): Propriedades customizadas (ex: {"property1": "valor"})
            properties (dict, opcional): Propriedades customizadas (ex: {"property1": "valor"})
            provider_name (str, opcional): Nome do provedor Delta Sharing
            share_name (str, opcional): Nome do share
            storage_root (str, opcional): URL raiz de armazenamento
        """
        try:
            return get_catalog_client().create_catalog(
                name=name,
                comment=comment,
                connection_name=connection_name,
                options=options,
                properties=properties,
                provider_name=provider_name,
                share_name=share_name,
                storage_root=storage_root
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None:
                try:
                    detail = e.response.json()
                except Exception:
                    detail = e.response.text
                raise Exception(f"Failed to create catalog: {detail}")
            raise Exception(f"Failed to create catalog: {str(e)}")
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to create catalog: {str(e)}")

    @mcp.tool()
    def delete_catalog(name: str, force: bool = False) -> Dict:
        """
        Deleta um catálogo no Databricks. Use force=True para forçar a exclusão mesmo se houver dependências.
        Parâmetros:
            name (str): Nome do catálogo a ser deletado
            force (bool, opcional): Se True, força a exclusão (default: False)
        """
        try:
            return get_catalog_client().delete_catalog(name, force=force)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to delete catalog: {str(e)}")

    @mcp.resource("catalog://{catalog_name}")
    def get_catalog_info(catalog_name: str) -> Dict:
        try:
            return get_catalog_client().get_catalog_info(catalog_name)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed to get catalog info: {str(e)}")

    return mcp