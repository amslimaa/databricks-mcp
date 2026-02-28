"""
Databricks MCP Server.
"""

import argparse
import os
from mcp.server.fastmcp import FastMCP
from catalogs import mcp_tools as catalogs_tools
from schemas import mcp_tools as schemas_tools
from resources import mcp_tools as resources_tools
from queries import mcp_tools as queries_tools
from tables import mcp_tools as tables_tools
from context import set_request_credentials, clear_request_credentials

PORT = int(os.getenv("PORT", 8080))

parser = argparse.ArgumentParser()
parser.add_argument(
    "--transport",
    choices=["stdio", "streamable-http", "sse"],
    default="streamable-http",
)
args, _ = parser.parse_known_args()

mcp = FastMCP("Databricks MCP", host="0.0.0.0", port=PORT)


@mcp.middleware("request")
async def credentials_middleware(request, call_next):
    host = request.headers.get("X-Databricks-Host")
    token = request.headers.get("X-Databricks-Token")
    set_request_credentials(host, token)
    response = await call_next(request)
    clear_request_credentials()
    return response


catalogs_tools(mcp)
schemas_tools(mcp)
resources_tools(mcp)
queries_tools(mcp)
tables_tools(mcp)

if __name__ == "__main__":
    transport = args.transport
    if transport == "stdio":
        mcp.run(transport="stdio")
    elif transport == "sse":
        mcp.run(transport="sse")
    else:
        mcp.run(transport="streamable-http")
