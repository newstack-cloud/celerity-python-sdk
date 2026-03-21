"""Tests for SQL database parameter types and helpers."""

from __future__ import annotations

from typing import Annotated, get_args

from celerity.resources._tokens import resolve_marker_token
from celerity.resources.sql_database import (
    DEFAULT_SQL_CREDENTIALS_TOKEN,
    DEFAULT_SQL_READER_TOKEN,
    DEFAULT_SQL_WRITER_TOKEN,
    SqlCredentials,
    SqlCredentialsMarker,
    SqlDatabase,
    SqlDatabaseCredentials,
    SqlDatabaseParam,
    SqlReader,
    SqlReaderParam,
    SqlWriter,
    SqlWriterParam,
    sql_credentials_token,
    sql_instance_token,
    sql_reader_token,
    sql_writer_token,
)


class TestSqlWriterParam:
    def test_default_marker(self) -> None:
        param = SqlWriterParam()
        assert param.resource_type == "sql:writer"
        assert param.resource_name is None

    def test_named_marker(self) -> None:
        param = SqlWriterParam("mydb")
        assert param.resource_type == "sql:writer"
        assert param.resource_name == "mydb"

    def test_resolve_default_token(self) -> None:
        token = resolve_marker_token(SqlWriterParam())
        assert token == "celerity:sql:writer:default"

    def test_resolve_named_token(self) -> None:
        token = resolve_marker_token(SqlWriterParam("mydb"))
        assert token == "celerity:sql:writer:mydb"


class TestSqlReaderParam:
    def test_default_marker(self) -> None:
        param = SqlReaderParam()
        assert param.resource_type == "sql:reader"
        assert param.resource_name is None

    def test_named_marker(self) -> None:
        param = SqlReaderParam("mydb")
        assert param.resource_type == "sql:reader"
        assert param.resource_name == "mydb"

    def test_resolve_default_token(self) -> None:
        token = resolve_marker_token(SqlReaderParam())
        assert token == "celerity:sql:reader:default"

    def test_resolve_named_token(self) -> None:
        token = resolve_marker_token(SqlReaderParam("mydb"))
        assert token == "celerity:sql:reader:mydb"


class TestSqlDatabaseParam:
    def test_default_marker(self) -> None:
        param = SqlDatabaseParam()
        assert param.resource_type == "sql:writer"
        assert param.resource_name is None

    def test_named_marker(self) -> None:
        param = SqlDatabaseParam("mydb")
        assert param.resource_type == "sql:writer"
        assert param.resource_name == "mydb"

    def test_same_resource_type_as_writer(self) -> None:
        assert SqlDatabaseParam().resource_type == SqlWriterParam().resource_type

    def test_resolve_default_token(self) -> None:
        token = resolve_marker_token(SqlDatabaseParam())
        assert token == "celerity:sql:writer:default"

    def test_resolve_named_token(self) -> None:
        token = resolve_marker_token(SqlDatabaseParam("mydb"))
        assert token == "celerity:sql:writer:mydb"


class TestSqlCredentialsMarker:
    def test_default_marker(self) -> None:
        param = SqlCredentialsMarker()
        assert param.resource_type == "sql:credentials"
        assert param.resource_name is None

    def test_named_marker(self) -> None:
        param = SqlCredentialsMarker("mydb")
        assert param.resource_type == "sql:credentials"
        assert param.resource_name == "mydb"


class TestTokenFactories:
    def test_writer_token(self) -> None:
        assert sql_writer_token("mydb") == "celerity:sql:writer:mydb"

    def test_reader_token(self) -> None:
        assert sql_reader_token("mydb") == "celerity:sql:reader:mydb"

    def test_credentials_token(self) -> None:
        assert sql_credentials_token("mydb") == "celerity:sql:credentials:mydb"

    def test_instance_token(self) -> None:
        assert sql_instance_token("mydb") == "celerity:sql:instance:mydb"


class TestDefaultTokenConstants:
    def test_writer(self) -> None:
        assert DEFAULT_SQL_WRITER_TOKEN == "celerity:sql:writer:default"

    def test_reader(self) -> None:
        assert DEFAULT_SQL_READER_TOKEN == "celerity:sql:reader:default"

    def test_credentials(self) -> None:
        assert DEFAULT_SQL_CREDENTIALS_TOKEN == "celerity:sql:credentials:default"


class TestAnnotatedAliases:
    def test_sql_writer(self) -> None:
        args = get_args(SqlWriter)
        assert isinstance(args[1], SqlWriterParam)
        assert args[1].resource_name is None

    def test_sql_reader(self) -> None:
        args = get_args(SqlReader)
        assert isinstance(args[1], SqlReaderParam)
        assert args[1].resource_name is None

    def test_sql_database(self) -> None:
        args = get_args(SqlDatabase)
        assert isinstance(args[1], SqlDatabaseParam)
        assert args[1].resource_name is None

    def test_sql_credentials(self) -> None:
        args = get_args(SqlCredentials)
        assert isinstance(args[1], SqlCredentialsMarker)
        assert args[1].resource_name is None

    def test_named_writer(self) -> None:
        from sqlalchemy.ext.asyncio import AsyncEngine

        named = Annotated[AsyncEngine, SqlWriterParam("orders")]
        args = get_args(named)
        assert args[0] is AsyncEngine
        assert isinstance(args[1], SqlWriterParam)
        assert args[1].resource_name == "orders"

    def test_named_reader(self) -> None:
        from sqlalchemy.ext.asyncio import AsyncEngine

        named = Annotated[AsyncEngine, SqlReaderParam("orders")]
        args = get_args(named)
        assert args[0] is AsyncEngine
        assert isinstance(args[1], SqlReaderParam)
        assert args[1].resource_name == "orders"

    def test_named_database(self) -> None:
        from sqlalchemy.ext.asyncio import AsyncEngine

        named = Annotated[AsyncEngine, SqlDatabaseParam("orders")]
        args = get_args(named)
        assert args[0] is AsyncEngine
        assert isinstance(args[1], SqlDatabaseParam)
        assert args[1].resource_name == "orders"

    def test_named_credentials(self) -> None:
        named = Annotated[SqlDatabaseCredentials, SqlCredentialsMarker("orders")]
        args = get_args(named)
        assert args[0] is SqlDatabaseCredentials
        assert isinstance(args[1], SqlCredentialsMarker)
        assert args[1].resource_name == "orders"
