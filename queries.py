"""
Databricks Queries Module: Ferramentas para executar queries SQL no Databricks.

Este módulo utiliza a API de Statement Execution do Databricks para submeter
consultas SQL, monitorar seu status de forma assíncrona e buscar os resultados.
Ele abstrai a complexidade do polling e do tratamento de resultados, que podem
ser retornados diretamente (inline) ou através de links externos (external_links).
"""
import os
import time
import logging
from typing import Dict

import requests
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class DatabricksQueryClient:
    """Client for the Databricks Statement Execution API."""

    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN are required"
            )
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        # Note: This API version is 2.0
        self.base_url = f"{self.host}/api/2.0/sql/statements"

    def execute_statement(self, warehouse_id: str, statement: str) -> Dict:
        """Submits a SQL statement for execution."""
        payload = {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "0s",  # Returns immediately
            "disposition": "EXTERNAL_LINKS" # Recommended for fetching results
        }
        logger.debug(f"Submitting statement to {self.base_url}")
        response = requests.post(self.base_url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def get_statement(self, statement_id: str) -> Dict:
        """Gets the status of a statement execution."""
        url = f"{self.base_url}/{statement_id}"
        logger.debug(f"Getting statement status from {url}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def _fetch_results_from_links(self, result: Dict) -> Dict:
        """
        Busca e consolida os dados de resultados a partir de links externos.
        Se os dados já estiverem no formato 'inline', retorna-os diretamente.
        """
        if "external_links" not in result:
            # If data is inline, just return it
            return result

        all_data = []
        # Use a new requests.Session for potential connection reuse
        with requests.Session() as session:
            for link_info in result["external_links"]:
                # The external link does not require auth headers
                logger.info(f"Fetching results from external link (chunk {link_info['chunk_index']})...")
                response = session.get(link_info["external_link"])
                response.raise_for_status()
                # The data is returned as a JSON array of arrays
                all_data.extend(response.json())

        # Reconstruct the result object to match the INLINE format
        final_result = {
            "data_array": all_data,
            "row_count": len(all_data)
        }
        return final_result


# --- Lazy Initialization of the client ---
_query_client_instance = None

def get_query_client():
    """Lazily initializes and returns the DatabricksQueryClient."""
    global _query_client_instance
    if _query_client_instance is None:
        _query_client_instance = DatabricksQueryClient()
    return _query_client_instance


def mcp_tools(mcp: FastMCP):
    """Registers query-related tools with the MCP server."""

    @mcp.tool()
    def execute_sql_query(warehouse_id: str, sql_query: str, timeout_seconds: int = 300) -> Dict:
        """
        Executa uma query SQL em um SQL Warehouse e aguarda o resultado.

        Esta ferramenta submete a query, monitora o status da execução e,
        após o sucesso, busca os dados (inclusive de links externos) e os retorna.
        Utilize a ferramenta 'list_sql_warehouses' para obter um 'warehouse_id' válido.

        Args:
            warehouse_id (str): O ID do SQL Warehouse onde a query será executada.
            sql_query (str): A instrução SQL a ser executada.
            timeout_seconds (int): O tempo máximo em segundos para aguardar a conclusão da query.

        Returns:
            Dict: Um dicionário contendo o resultado, com a seguinte estrutura:
                  - 'schema' (dict): Descreve as colunas do resultado.
                  - 'data_array' (list): Uma lista de listas, onde cada lista interna representa uma linha.
                  - 'row_count' (int): O número total de linhas retornadas.
        """
        try:
            logger.info(f"Executing SQL query on warehouse {warehouse_id}: \"{sql_query[:100]}...\"")
            client = get_query_client()
            # 1. Submit the query
            submission_response = client.execute_statement(warehouse_id, sql_query)
            statement_id = submission_response["statement_id"]
            logger.info(f"Query submitted. Statement ID: {statement_id}")

            # 2. Poll for the result
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                status_response = client.get_statement(statement_id)
                status = status_response["status"]["state"]
                logger.debug(f"Polling statement {statement_id}. Current status: {status}")

                if status == "SUCCEEDED":
                    logger.info(f"Query {statement_id} succeeded. Fetching results...")
                    # Process the result to fetch data from external links if necessary
                    result = status_response.get("result", {})
                    manifest = status_response.get("manifest", {})
                    
                    final_data = client._fetch_results_from_links(result)
                    final_data["schema"] = manifest.get("schema", {})
                    logger.info(f"Successfully fetched {final_data.get('row_count', 0)} rows.")
                    return final_data
                elif status in ["FAILED", "CANCELED", "CLOSED"]:
                    error_msg = status_response.get('status', {}).get('error', {}).get('message', 'Unknown error')
                    logger.error(f"Query {statement_id} failed with status '{status}'. Reason: {error_msg}")
                    raise Exception(f"Query failed with status '{status}': {error_msg}")
                
                time.sleep(2) # Wait 2 seconds before polling again

            logger.warning(f"Query {statement_id} timed out after {timeout_seconds} seconds.")
            raise TimeoutError(f"Query timed out after {timeout_seconds} seconds.")

        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error(f"API request failed during query execution: {e}", exc_info=True)
            raise Exception(f"API request failed during query execution: {e}")

    return mcp
