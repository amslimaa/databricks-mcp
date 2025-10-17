

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

- Python 3.9 ou superior
- Acesso a um workspace Databricks com o Unity Catalog habilitado
- Um token de acesso pessoal do Databricks

### Instalação

Este projeto utiliza o `uv` como gerenciador de pacotes e ambientes virtuais.

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/seu-usuario/databricks-mcp.git
    cd databricks-mcp
    ```

2.  **Crie o ambiente virtual e instale as dependências:**
    O `uv` simplifica a criação do ambiente e a instalação. O comando a seguir lê o `pyproject.toml` e instala as dependências necessárias.
    ```bash
    uv pip install -r requirements.txt
    ```

### Configuração

1.  Crie um arquivo chamado `.env` na raiz do projeto.
2.  Adicione suas credenciais do Databricks ao arquivo `.env`:

    ```env
    DATABRICKS_HOST="https://seu-workspace.databricks.net"
    DATABRICKS_TOKEN="seu-token-de-acesso-pessoal"
    ```

    - `DATABRICKS_HOST`: A URL do seu workspace Databricks.
    - `DATABRICKS_TOKEN`: Seu token de acesso pessoal.

## 🛠️ Uso

Para usar este servidor com um cliente MCP (como o Gemini CLI no VS Code), você precisará configurar o cliente para iniciar o servidor. A configuração informa ao cliente qual comando executar.

Adicione o seguinte objeto `mcpServers` às suas configurações de usuário ou do workspace (por exemplo, em `.vscode/settings.json`):

```json
{
  "mcpServers": {
    "Databricks MCP": {
      "command": "<caminho_para_seu_uv.EXE>",
      "args": [
        "run",
        "--with",
        "requests",
        "--with",
        "argparse",
        "--with",
        "mcp[cli]",
        "mcp",
        "run",
        "<caminho_para_seu_main.py>"
      ]
    }
  }
}
```

**Observações:**

*   **Caminho do Comando:** Substitua `<caminho_para_seu_uv.EXE>` e `<caminho_para_seu_main.py>` pelos caminhos absolutos corretos em sua máquina.
*   **Ativação:** Uma vez configurado, seu cliente MCP (por exemplo, a extensão Gemini no VS Code) irá iniciar o servidor automaticamente quando você interagir com ele neste projeto.

O método de execução anterior (`uv run python main.py stdio`) é destinado a testes locais e não para integração com um cliente MCP.

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

