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

# Note: This argument parsing might not work as expected if the runner
# (e.g., 'mcp run') does not forward unknown arguments to the script.
# If you encounter issues, consider using a .env file as described in README.md.
parser = argparse.ArgumentParser()
parser.add_argument("--db-host")
parser.add_argument("--db-token")
args, _ = parser.parse_known_args()

if args.db_host:
    os.environ['DATABRICKS_HOST'] = args.db_host
if args.db_token:
    os.environ['DATABRICKS_TOKEN'] = args.db_token

mcp = FastMCP("Databricks MCP")
catalogs_tools(mcp)
schemas_tools(mcp)
resources_tools(mcp)
queries_tools(mcp)
tables_tools(mcp)