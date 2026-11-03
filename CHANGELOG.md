# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-03

### Added
- **Transaction Runner**: Robust transactional execution engine with exponential backoff, full jitter, classification of retryable PostgreSQL errors (40001 serialization failure, 40P01 deadlock detected), and non-retryable isolation handling.
- **SQLAlchemy Integration**: Native `SQLAlchemySessionAdapter` and `run_sqlalchemy_transaction` helper with rollback state reset (`expunge_all`), plus `SQLAlchemyQueryExecutor` for diagnostic execution.
- **asyncpg Integration**: Dedicated `AsyncpgConnectionAdapter` and `run_asyncpg_transaction` helper with transaction boundary safety, plus `AsyncpgQueryExecutor` for diagnostic queries.
- **Diagnostics & Observability**: Real-time PostgreSQL session activity inspection, wait state monitoring, authoritative lock graph analysis (`detect_blocking_chains`), and structured logging with automated credential sanitization.
- **Strict Typing & Developer Experience**: 100% type-annotated codebase with PEP 561 `py.typed` marker, zero-warning strict Mypy and Pyright verification, blocking Ruff linter, and real integration tests.

