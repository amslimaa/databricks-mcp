"""
Databricks MCP Server.
"""

import argparse
import os
import sys
from mcp.server.fastmcp import FastMCP
from catalogs import mcp_tools as catalogs_tools
from schemas import mcp_tools as schemas_tools
from resources import mcp_tools as resources_tools
from queries import mcp_tools as queries_tools
from tables import mcp_tools as tables_tools

PORT = int(os.getenv("PORT", 8080))

parser = argparse.ArgumentParser()
parser.add_argument(
    "--transport", choices=["stdio", "streamable-http"], default="streamable-http"
)
parser.add_argument("--db-host")
parser.add_argument("--db-token")
args, _ = parser.parse_known_args()

if args.db_host:
    os.environ["DATABRICKS_HOST"] = args.db_host
if args.db_token:
    os.environ["DATABRICKS_TOKEN"] = args.db_token

mcp = FastMCP("Databricks MCP", host="0.0.0.0", port=PORT)
catalogs_tools(mcp)
schemas_tools(mcp)
resources_tools(mcp)
queries_tools(mcp)
tables_tools(mcp)

if __name__ == "__main__":
    transport = args.transport
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="streamable-http")
