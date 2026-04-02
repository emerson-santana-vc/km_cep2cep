import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

try:
    import oracledb
except ImportError:  # pragma: no cover - handled at runtime
    oracledb = None


SAFE_SQL_PATTERN = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)
FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|BEGIN|DECLARE|EXEC|EXECUTE|CALL|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)
SQL_BIND_PATTERN = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


@dataclass
class OracleConfig:
    host: str
    port: int
    service_name: str
    user: str
    password: str
    sql_query: str


class OracleConfigError(ValueError):
    pass


class OracleQueryError(RuntimeError):
    pass


def _read_sql_from_file(sql_file: str) -> str:
    path = Path(sql_file).expanduser()
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").strip()


def load_oracle_config() -> OracleConfig:
    host = (os.getenv("ORACLE_HOST") or "").strip()
    port_raw = (os.getenv("ORACLE_PORT") or "").strip()
    service_name = (os.getenv("ORACLE_SERVICE_NAME") or "").strip()
    user = (os.getenv("ORACLE_USER") or "").strip()
    password = os.getenv("ORACLE_PASSWORD") or ""
    sql_file = (os.getenv("ORACLE_SQL_QUERY_FILE") or "").strip()
    sql_query = _read_sql_from_file(sql_file) if sql_file else ""
    if not sql_query:
        sql_query = (os.getenv("ORACLE_SQL_QUERY") or "").strip()

    missing = [
        name
        for name, value in {
            "ORACLE_HOST": host,
            "ORACLE_PORT": port_raw,
            "ORACLE_SERVICE_NAME": service_name,
            "ORACLE_USER": user,
            "ORACLE_PASSWORD": password,
            "ORACLE_SQL_QUERY": sql_query,
        }.items()
        if not value
    ]
    if missing:
        raise OracleConfigError(f"Variáveis Oracle ausentes: {', '.join(missing)}")

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise OracleConfigError("ORACLE_PORT deve ser um número inteiro.") from exc

    validate_oracle_sql(sql_query)
    return OracleConfig(
        host=host,
        port=port,
        service_name=service_name,
        user=user,
        password=password,
        sql_query=sql_query,
    )


def validate_oracle_sql(sql_query: str) -> None:
    normalized = (sql_query or "").strip()
    if not normalized:
        raise OracleConfigError("ORACLE_SQL_QUERY não pode ficar vazio.")
    if ";" in normalized:
        raise OracleConfigError("ORACLE_SQL_QUERY deve conter apenas uma instrução SQL, sem ponto e vírgula.")
    if not SAFE_SQL_PATTERN.match(normalized):
        raise OracleConfigError("ORACLE_SQL_QUERY deve começar com SELECT ou WITH.")
    if FORBIDDEN_SQL_PATTERN.search(normalized):
        raise OracleConfigError("ORACLE_SQL_QUERY contém comandos não permitidos.")


def parse_bind_params(raw_json: str) -> dict[str, Any]:
    text = (raw_json or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise OracleQueryError("Parâmetros Oracle devem estar em JSON válido.") from exc
    if not isinstance(data, dict):
        raise OracleQueryError("Parâmetros Oracle devem ser um objeto JSON com pares chave/valor.")
    return data


def extract_sql_bind_names(sql_query: str) -> list[str]:
    if not sql_query:
        return []
    bind_names = []
    for name in SQL_BIND_PATTERN.findall(sql_query):
        if name not in bind_names:
            bind_names.append(name)
    return bind_names


def build_dsn(config: OracleConfig) -> str:
    return f"{config.host}:{config.port}/{config.service_name}"


def execute_oracle_query(sql_query: str, bind_params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    if oracledb is None:
        raise OracleQueryError("Dependência 'oracledb' não está instalada no ambiente.")

    config = load_oracle_config()
    validate_oracle_sql(sql_query)

    binds = bind_params or {}
    dsn = build_dsn(config)

    try:
        with oracledb.connect(user=config.user, password=config.password, dsn=dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, binds)
                columns = [col[0].lower() for col in cursor.description or []]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    except OracleConfigError:
        raise
    except Exception as exc:
        raise OracleQueryError(f"Falha ao executar consulta Oracle: {exc}") from exc
