

# Databricks MCP

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

O **Databricks MCP** é um servidor de protocolo de contexto de modelo (MCP) que fornece um conjunto de ferramentas poderosas e intuitivas para interagir com seu ambiente Databricks. Ele simplifica o gerenciamento de recursos do Unity Catalog e a execução de consultas, expondo funcionalidades complexas da API Databricks como ferramentas fáceis de usar.

Este projeto foi projetado para ser usado como um agente inteligente ou um backend para aplicações que precisam automatizar tarefas no Databricks.

## ✨ Recursos

- **Gerenciamento de Catálogos**: Crie, liste e exclua catálogos do Unity Catalog.
- **Gerenciamento de Schemas**: Crie, liste, atualize e exclua schemas dentro dos catálogos.
- **Execução de SQL**: Execute consultas SQL em seus SQL Warehouses e obtenha os resultados de forma síncrona.
- **Inspeção de Recursos**: Liste os SQL Warehouses disponíveis em seu workspace.
- **Arquitetura Modular**: O código é organizado em módulos (`catalogs`, `schemas`, `queries`, `resources`), facilitando a manutenção e a extensão.
- **Configuração Simples**: Utilize um arquivo `.env` para gerenciar suas credenciais do Databricks de forma segura.

## 🚀 Começando

Siga estas instruções para configurar e executar o Databricks MCP em seu ambiente local.

### Pré-requisitos

- Python 3.14+ (ou Docker)
- Acesso a um workspace Databricks com o Unity Catalog habilitado
- Um token de acesso pessoal do Databricks

### Instalação Local (Python)

Este projeto utiliza o `uv` como gerenciador de pacotes e ambientes virtuais.

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/amslimaa/databricks-mcp.git

    cd databricks-mcp
    ```

2.  **Crie o ambiente virtual e instale as dependências:**
    ```bash
    uv pip install -e .
    ```

### Instalação com Docker

```bash
# Build da imagem
docker build -t databricks-mcp .

# Executar o container
docker run -d -p 8080:8080 databricks-mcp

# Ou usar docker-compose
docker-compose up -d
```

### Configuração

#### Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto (para desenvolvimento local):

```env
DATABRICKS_HOST="https://seu-workspace.databricks.net"
DATABRICKS_TOKEN="seu-token-de-acesso-pessoal"
```

- `DATABRICKS_HOST`: A URL do seu workspace Databricks.
- `DATABRICKS_TOKEN`: Seu token de acesso pessoal.

#### Configuração do Cliente MCP

**VSCode / Cursor:**

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "url": "http://localhost:8080/mcp",
      "env": {
        "DATABRICKS_HOST": "https://seu-workspace.databricks.net",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

**OpenCode:**

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

**Railway (Produção):**

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "url": "https://seu-projeto.up.railway.app/mcp",
      "env": {
        "DATABRICKS_HOST": "https://seu-workspace.databricks.net",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

## 🛠️ Uso

### Desenvolvimento Local

```bash
# Modo stdio (para testes)
uv run python main.py stdio

# Modo HTTP (StreamableHTTP)
uv run python main.py
```

### Docker

```bash
# Development
docker build -t databricks-mcp .
docker run -d -p 8080:8080 -e PORT=8080 databricks-mcp

# Testar endpoint
curl http://localhost:8080/mcp
```

### Deploy no Railway

1. Conecte o repositório GitHub ao Railway
2. Railway detecta automaticamente o `Dockerfile`
3. Não é necessário configurar variáveis de ambiente (credenciais vêm do cliente)
4. O endpoint será `https://SEU-PROJETO.up.railway.app/mcp`

### Ferramentas Disponíveis

Aqui está uma visão geral das ferramentas disponíveis, agrupadas por módulo:

#### 📚 Catálogos (`catalogs.py`)

- `list_catalogs(page_token: str = None) -> dict`: Lista todos os catálogos no workspace.
- `create_catalog(name: str, comment: str = "", ...) -> dict`: Cria um novo catálogo.
- `delete_catalog(name: str, force: bool = False) -> dict`: Exclui um catálogo.
- `resource: "catalog://{catalog_name}"`: Obtém informações detalhadas sobre um catálogo específico.

#### 🗂️ Schemas (`schemas.py`)

- `list_schemas(catalog_name: str) -> dict`: Lista todos os schemas em um catálogo.
- `create_schema(catalog_name: str, name: str, ...) -> dict`: Cria um novo schema.
- `update_schema(full_name: str, new_name: str = None, ...) -> dict`: Atualiza um schema existente.
- `delete_schema(full_name: str) -> dict`: Exclui um schema.
- `resource: "schema://{catalog_name}.{schema_name}"`: Obtém informações sobre um schema específico.

#### ⚙️ Recursos (`resources.py`)

- `list_sql_warehouses() -> dict`: Lista todos os SQL Warehouses disponíveis para encontrar um `warehouse_id`.

#### ❓ Consultas (`queries.py`)

- `execute_sql_query(warehouse_id: str, sql_query: str, timeout_seconds: int = 300) -> dict`: Executa uma consulta SQL em um warehouse e aguarda o resultado.

#
## 🤝 Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir uma issue ou enviar um pull request.

