"""Unit tests for error classifier subsystem."""

import pytest
from pgshield.retry.classifier import (
    DEFAULT_RETRYABLE_SQLSTATES,
    SQLSTATE_CHECK_VIOLATION,
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_FOREIGN_KEY_VIOLATION,
    SQLSTATE_NOT_NULL_VIOLATION,
    SQLSTATE_SERIALIZATION_FAILURE,
    SQLSTATE_UNIQUE_VIOLATION,
    ClassificationResult,
    ErrorCategory,
    classify_error,
    extract_sqlstate,
    is_retryable,
)


class DummyDriverError(Exception):
    """Mock database driver exception."""

    def __init__(self, message: str, sqlstate: str | None = None, pgcode: str | None = None, code: str | None = None):
        super().__init__(message)
        if sqlstate is not None:
            self.sqlstate = sqlstate
        if pgcode is not None:
            self.pgcode = pgcode
        if code is not None:
            self.code = code

DummyDriverError.__module__ = "asyncpg.exceptions"


class DummyWrappedError(Exception):
    """Mock ORM/wrapper exception like SQLAlchemy DBAPIError."""

    def __init__(self, message: str, orig: Exception | None = None):
        super().__init__(message)
        self.orig = orig

DummyWrappedError.__module__ = "sqlalchemy.exc"


class TestSQLStateExtraction:
    """Test SQLSTATE code extraction across different driver formats and nesting."""

    def test_extract_direct_sqlstate_attribute(self):
        err = DummyDriverError("serialization failure", sqlstate="40001")
        assert extract_sqlstate(err) == "40001"

    def test_extract_direct_pgcode_attribute(self):
        err = DummyDriverError("deadlock detected", pgcode="40p01")
        assert extract_sqlstate(err) == "40P01"

    def test_extract_direct_code_attribute(self):
        err = DummyDriverError("unique violation", code="23505")
        assert extract_sqlstate(err) == "23505"

    def test_extract_from_wrapped_orig(self):
        inner = DummyDriverError("inner error", sqlstate="40001")
        wrapper = DummyWrappedError("wrapper error", orig=inner)
        assert extract_sqlstate(wrapper) == "40001"

    def test_extract_from_cause_chain(self):
        inner = DummyDriverError("inner error", sqlstate="40P01")
        try:
            try:
                raise inner
            except Exception as e:
                raise RuntimeError("higher level error") from e
        except Exception as wrapper:
            assert extract_sqlstate(wrapper) == "40P01"

    def test_extract_from_context_chain(self):
        inner = DummyDriverError("inner error", sqlstate="40001")
        try:
            try:
                raise inner
            except Exception:
                raise RuntimeError("contextual error")
        except Exception as wrapper:
            assert extract_sqlstate(wrapper) == "40001"

    def test_extract_returns_none_for_standard_exceptions(self):
        assert extract_sqlstate(ValueError("bad value")) is None
        assert extract_sqlstate(RuntimeError("runtime issue")) is None
        assert extract_sqlstate(None) is None  # type: ignore

    def test_extract_ignores_invalid_code_lengths(self):
        err = DummyDriverError("bad", sqlstate="123")  # Not 5 chars
        assert extract_sqlstate(err) is None


class TestErrorClassifier:
    """Test error classification rules."""

    def test_serialization_failure_40001_is_retryable(self):
        err = DummyDriverError("could not serialize access", sqlstate="40001")
        result = classify_error(err)

        assert result.retryable is True
        assert result.sqlstate == "40001"
        assert result.category == ErrorCategory.SERIALIZATION_FAILURE
        assert is_retryable(err) is True

    def test_deadlock_detected_40p01_is_retryable(self):
        err = DummyDriverError("deadlock detected", pgcode="40P01")
        result = classify_error(err)

        assert result.retryable is True
        assert result.sqlstate == "40P01"
        assert result.category == ErrorCategory.DEADLOCK_DETECTED
        assert is_retryable(err) is True

    def test_unique_violation_23505_is_not_retryable(self):
        err = DummyDriverError("duplicate key", sqlstate=SQLSTATE_UNIQUE_VIOLATION)
        result = classify_error(err)

        assert result.retryable is False
        assert result.sqlstate == "23505"
        assert result.category == ErrorCategory.INTEGRITY_VIOLATION
        assert is_retryable(err) is False

    def test_foreign_key_violation_23503_is_not_retryable(self):
        err = DummyDriverError("fk violation", sqlstate=SQLSTATE_FOREIGN_KEY_VIOLATION)
        result = classify_error(err)

        assert result.retryable is False
        assert result.sqlstate == "23503"
        assert result.category == ErrorCategory.INTEGRITY_VIOLATION
        assert is_retryable(err) is False

    def test_check_violation_23514_is_not_retryable(self):
        err = DummyDriverError("check violation", sqlstate=SQLSTATE_CHECK_VIOLATION)
        result = classify_error(err)

        assert result.retryable is False
        assert result.sqlstate == "23514"
        assert result.category == ErrorCategory.INTEGRITY_VIOLATION
        assert is_retryable(err) is False

    def test_not_null_violation_23502_is_not_retryable(self):
        err = DummyDriverError("not null violation", sqlstate=SQLSTATE_NOT_NULL_VIOLATION)
        result = classify_error(err)

        assert result.retryable is False
        assert result.sqlstate == "23502"
        assert result.category == ErrorCategory.INTEGRITY_VIOLATION
        assert is_retryable(err) is False

    def test_arbitrary_exception_is_not_retryable(self):
        err = ZeroDivisionError("division by zero")
        result = classify_error(err)

        assert result.retryable is False
        assert result.sqlstate is None
        assert result.category == ErrorCategory.UNKNOWN
        assert is_retryable(err) is False

    def test_custom_retryable_sqlstates_override(self):
        err = DummyDriverError("syntax error", sqlstate="42601")
        # By default not retryable
        assert is_retryable(err) is False

        # With custom override set
        custom_set = {"42601"}
        result = classify_error(err, retryable_sqlstates=custom_set)
        assert result.retryable is True
        assert is_retryable(err, retryable_sqlstates=custom_set) is True

    def test_custom_extractor(self):
        class SpecialError(Exception):
            def custom_code(self):
                return "40001"

        err = SpecialError("special serialization")
        assert is_retryable(err) is False

        custom_extractor = lambda e: getattr(e, "custom_code", lambda: None)()
        result = classify_error(err, custom_extractor=custom_extractor)
        assert result.retryable is True
        assert result.sqlstate == "40001"

    def test_extract_sqlstate_with_cyclic_cause_does_not_hang(self):
        err = Exception("cyclic error")
        err.__cause__ = err  # Cycle!
        assert extract_sqlstate(err) is None

    def test_fake_error_with_code_not_classified_retryable(self):
        """A plain exception carrying code='40001' without PostgreSQL driver evidence is not retryable."""
        class FakeError(Exception):
            code = "40001"

        err = FakeError("fake error with code 40001")
        assert extract_sqlstate(err) is None
        result = classify_error(err)
        assert result.retryable is False
        assert result.sqlstate is None
        assert result.category == ErrorCategory.UNKNOWN
        assert is_retryable(err) is False

    def test_mock_driver_error_in_wrong_module_not_retryable(self):
        """A class named MockDriverError but in the wrong module is NOT retryable."""
        class MockDriverError(Exception):
            code = "40001"
        MockDriverError.__module__ = "myapp.errors"
        result = classify_error(MockDriverError("x"))
        assert result.retryable is False
        assert result.sqlstate is None
