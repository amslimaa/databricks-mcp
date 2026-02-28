FROM python:3.14-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml .
RUN uv pip install --system -e .

COPY catalogs.py main.py schemas.py tables.py queries.py resources.py ./

EXPOSE 8080

CMD ["python", "main.py"]
