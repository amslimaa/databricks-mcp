# AGENTS.md - Development Guide for Databricks MCP

This document provides guidelines for agentic coding agents working in this repository.

## Project Overview

Databricks MCP is a Model Context Protocol server that provides tools for interacting with Databricks Unity Catalog (catalogs, schemas, tables) and executing SQL queries. Built with Python 3.14+, using the MCP SDK.

## Build/Lint/Test Commands

### Running the Server

```bash
# Using uv (recommended)
uv run python main.py stdio

# Or with specific environment variables
DATABRICKS_HOST="https://..." DATABRICKS_TOKEN="..." uv run python main.py stdio

# HTTP transport (for remote deployment)
uv run python main.py
```

### Railway Deployment

The server supports StreamableHTTP transport for remote deployment on Railway:

1. **Deploy to Railway**:
   - Connect your GitHub repository to Railway
   - Railway automatically detects the `Dockerfile`
   - No environment variables required (credentials come from the client)

2. **Client Configuration** (Cursor/Claude/VSCode):
   ```json
   {
     "mcpServers": {
       "databricks-mcp": {
         "url": "https://your-project.up.railway.app/mcp",
         "env": {
           "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
           "DATABRICKS_TOKEN": "dapiyour-token-here"
         }
       }
     }
   }
   ```

3. **Local Docker Testing**:
   ```bash
   docker build -t databricks-mcp .
   docker run -p 8080:8080 -e PORT=8080 databricks-mcp
   ```

4. **Test the server**:
   ```bash
   curl http://localhost:8080/mcp
   ```

### Installing Dependencies

```bash
uv pip install -r requirements.txt
# Or from pyproject.toml
uv pip install -e .
```

### Testing

**No test framework is currently configured.** To add tests:

```bash
# Install pytest (recommended)
uv add --dev pytest pytest-asyncio

# Run all tests
pytest

# Run a single test file
pytest tests/test_catalogs.py

# Run a single test function
pytest tests/test_catalogs.py::test_list_catalogs -v
```

### Linting/Type Checking

**No linting tools are configured.** Recommended setup:

```bash
# Install ruff (fast linter/formatter)
uv add --dev ruff

# Run ruff
ruff check .
ruff check catalogs.py --fix

# Format code
ruff format .
```

## Code Style Guidelines

### General

- Python 3.14+ required
- Use type hints throughout (see `typing` module)
- Use docstrings for all public functions and classes
- Use logging for runtime information (see `queries.py` for example)

### Imports

Organize imports in this order (separate with blank lines):

1. Standard library (`os`, `time`, `logging`, `typing`)
2. Third-party packages (`requests`, `dotenv`, `mcp`)
3. Local modules (`from catalogs import ...`)

```python
import os
from typing import Dict, Optional

import requests
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from catalogs import mcp_tools as catalogs_tools
```

### Naming Conventions

- **Classes**: PascalCase (e.g., `DatabricksCatalogClient`)
- **Functions/variables**: snake_case (e.g., `list_catalogs`, `get_catalog_client`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `DEFAULT_TIMEOUT`)
- **Private members**: Prefix with underscore (e.g., `_catalog_client_instance`)

### Type Hints

Use the `typing` module for complex types:

```python
from typing import Dict, List, Optional

def list_catalogs(page_token: Optional[str] = None) -> Dict:
    ...

def create_catalog(
    name: str,
    comment: str = "",
    properties: Optional[dict] = None,
) -> Dict:
    ...
```

### Error Handling

Wrap API calls in try/except and re-raise as `Exception` with descriptive messages:

```python
try:
    return get_catalog_client().list_catalogs(page_token=page_token)
except (requests.exceptions.RequestException, ValueError) as e:
    raise Exception(f"Failed to list catalogs: {str(e)}")
```

For HTTP errors with response bodies:

```python
except requests.exceptions.HTTPError as e:
    if e.response is not None:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text
        raise Exception(f"Failed to create catalog: {detail}")
    raise Exception(f"Failed to create catalog: {str(e)}")
```

### Client Pattern

Use lazy initialization with global instance:

```python
_client_instance = None

def get_client():
    """Lazily initializes and returns the client."""
    global _client_instance
    if _client_instance is None:
        _client_instance = DatabricksClient()
    return _client_instance
```

### MCP Tools

Register tools using the `@mcp.tool()` decorator:

```python
def mcp_tools(mcp: FastMCP):
    @mcp.tool()
    def my_tool(param: str) -> Dict:
        """Tool description shown to users."""
        try:
            return get_client().do_something(param)
        except (requests.exceptions.RequestException, ValueError) as e:
            raise Exception(f"Failed: {e}")
    return mcp
```

Resources use `@mcp.resource()`:

```python
@mcp.resource("catalog://{catalog_name}")
def get_catalog_info(catalog_name: str) -> Dict:
    ...
```

### Configuration

- Use `.env` file for local development (see `.env` template)
- Load with `load_dotenv()` at module level
- Validate required environment variables in client `__init__`:

```python
def __init__(self):
    self.host = os.getenv("DATABRICKS_HOST")
    self.token = os.getenv("DATABRICKS_TOKEN")
    if not self.host or not self.token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN are required")
```

### File Organization

```
databricks-mcp/
├── main.py              # Entry point, FastMCP setup
├── catalogs.py          # Catalog CRUD + MCP tools
├── schemas.py           # Schema CRUD + MCP tools
├── tables.py            # Table CRUD + MCP tools
├── queries.py           # SQL execution + MCP tools
├── resources.py         # SQL warehouses + MCP tools
├── Dockerfile           # Railway deployment
├── docker-compose.yml   # Local Docker development
├── .dockerignore        # Docker build exclusions
├── opencode.json        # OpenCode MCP configuration
├── .env                 # Local config (not committed)
└── pyproject.toml       # Project config
```

### OpenCode Integration

Create `opencode.json` for OpenCode MCP client:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "databricks-mcp": {
      "type": "remote",
      "url": "http://localhost:8080/mcp",
      "enabled": true,
      "environment": {
        "DATABRICKS_HOST": "${DATABRICKS_HOST}",
        "DATABRICKS_TOKEN": "${DATABRICKS_TOKEN}"
      }
    }
  }
}
```

For Railway deployment, change URL to `https://your-project.up.railway.app/mcp`.

### VSCode Integration

The `.vscode/settings.json` contains MCP server configuration for development with VSCode extensions that support MCP.
