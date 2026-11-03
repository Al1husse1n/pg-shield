"""Unit tests for PostgreSQL error introspection and normalization."""

from pgshield.postgres.errors import (
    SQLSTATE_CONNECTION_FAILURE,
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_LOCK_NOT_AVAILABLE,
    SQLSTATE_QUERY_CANCELED,
    SQLSTATE_SERIALIZATION_FAILURE,
    SQLSTATE_UNIQUE_VIOLATION,
    PostgresErrorCategory,
    categorize_sqlstate,
    parse_postgres_error,
)


class MockDriverException(Exception):
    """Mock database driver exception with rich PostgreSQL attributes."""

    def __init__(
        self,
        message: str,
        sqlstate: str | None = None,
        detail: str | None = None,
        hint: str | None = None,
        table_name: str | None = None,
        column_name: str | None = None,
        constraint_name: str | None = None,
    ):
        super().__init__(message)
        if sqlstate is not None:
            self.sqlstate = sqlstate
        if detail is not None:
            self.detail = detail
        if hint is not None:
            self.hint = hint
        if table_name is not None:
            self.table_name = table_name
        if column_name is not None:
            self.column_name = column_name
        if constraint_name is not None:
            self.constraint_name = constraint_name


MockDriverException.__module__ = "asyncpg.exceptions"


class MockWrapperException(Exception):
    """Mock ORM exception with .orig attribute."""

    def __init__(self, message: str, orig: Exception):
        super().__init__(message)
        self.orig = orig


MockWrapperException.__module__ = "sqlalchemy.exc"


class TestPostgresErrors:
    """Test suite for PostgreSQL error categorization and parsing."""

    def test_serialization_failure_40001(self):
        err = MockDriverException(
            "could not serialize access", sqlstate=SQLSTATE_SERIALIZATION_FAILURE
        )
        details = parse_postgres_error(err)

        assert details.sqlstate == "40001"
        assert details.category == PostgresErrorCategory.SERIALIZATION_FAILURE
        assert details.is_serialization_failure is True
        assert details.is_deadlock is False
        assert details.is_concurrency_conflict is True
        assert details.driver_error_type == "MockDriverException"

    def test_deadlock_detected_40p01(self):
        err = MockDriverException("deadlock detected", sqlstate=SQLSTATE_DEADLOCK_DETECTED)
        details = parse_postgres_error(err)

        assert details.sqlstate == "40P01"
        assert details.category == PostgresErrorCategory.DEADLOCK_DETECTED
        assert details.is_serialization_failure is False
        assert details.is_deadlock is True
        assert details.is_concurrency_conflict is True

    def test_integrity_violations_with_metadata(self):
        err = MockDriverException(
            "duplicate key violates unique constraint users_email_key",
            sqlstate=SQLSTATE_UNIQUE_VIOLATION,
            detail="Key (email)=(alice@example.com) already exists.",
            table_name="users",
            column_name="email",
            constraint_name="users_email_key",
        )
        details = parse_postgres_error(err)

        assert details.sqlstate == "23505"
        assert details.category == PostgresErrorCategory.INTEGRITY_VIOLATION
        assert details.is_concurrency_conflict is False
        assert details.table_name == "users"
        assert details.column_name == "email"
        assert details.constraint_name == "users_email_key"
        assert details.detail == "Key (email)=(alice@example.com) already exists."

    def test_lock_not_available(self):
        err_lock = MockDriverException(
            "could not obtain lock on row in relation", sqlstate=SQLSTATE_LOCK_NOT_AVAILABLE
        )
        assert parse_postgres_error(err_lock).category == PostgresErrorCategory.LOCK_NOT_AVAILABLE

    def test_connection_error_and_query_canceled(self):
        err_conn = MockDriverException(
            "connection to server lost", sqlstate=SQLSTATE_CONNECTION_FAILURE
        )
        assert parse_postgres_error(err_conn).category == PostgresErrorCategory.CONNECTION_ERROR

        err_cancel = MockDriverException(
            "canceling query due to user request", sqlstate=SQLSTATE_QUERY_CANCELED
        )
        assert parse_postgres_error(err_cancel).category == PostgresErrorCategory.QUERY_CANCELED

    def test_wrapped_exception_introspection(self):
        inner = MockDriverException(
            "inner deadlock", sqlstate="40P01", hint="See log for lock details"
        )
        outer = MockWrapperException("database operation failed", orig=inner)
        details = parse_postgres_error(outer)

        assert details.sqlstate == "40P01"
        assert details.category == PostgresErrorCategory.DEADLOCK_DETECTED
        assert details.hint == "See log for lock details"
        assert details.driver_error_type == "MockDriverException"

    def test_cause_chain_introspection(self):
        inner = MockDriverException("serialization error", sqlstate="40001")
        try:
            try:
                raise inner
            except Exception as e:
                raise RuntimeError("Service failure") from e
        except Exception as outer:
            details = parse_postgres_error(outer)
            assert details.sqlstate == "40001"
            assert details.category == PostgresErrorCategory.SERIALIZATION_FAILURE

    def test_unknown_and_non_exceptions(self):
        assert (
            parse_postgres_error(ValueError("bad value")).category == PostgresErrorCategory.UNKNOWN
        )
        assert parse_postgres_error(None).category == PostgresErrorCategory.UNKNOWN  # type: ignore

    def test_categorize_sqlstate_direct(self):
        assert categorize_sqlstate("40001") == PostgresErrorCategory.SERIALIZATION_FAILURE
        assert categorize_sqlstate("40p01") == PostgresErrorCategory.DEADLOCK_DETECTED
        assert categorize_sqlstate("23503") == PostgresErrorCategory.INTEGRITY_VIOLATION
        assert categorize_sqlstate("42P01") == PostgresErrorCategory.SYNTAX_OR_ACCESS_ERROR
        assert categorize_sqlstate(None) == PostgresErrorCategory.UNKNOWN

    def test_mock_driver_error_in_wrong_module_not_parsed_as_postgres(self):
        """MockDriverError in the wrong module has no driver evidence."""

        class MockDriverError(Exception):
            code = "40001"

        MockDriverError.__module__ = "myapp.errors"

        details = parse_postgres_error(MockDriverError("x"))
        assert details.sqlstate is None
        assert details.category == PostgresErrorCategory.UNKNOWN
