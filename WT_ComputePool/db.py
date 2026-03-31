from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, sessionmaker


load_dotenv()


DEFAULT_DATABASE_URL = (
    "postgresql+psycopg://username:password@localhost:5432/womentechies"
)


def normalize_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgresql+psycopg://"):
        return raw_url
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    return raw_url


DATABASE_URL = normalize_database_url(
    os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
)


def redact_database_url(raw_url: str) -> str:
    try:
        parsed = make_url(raw_url)
    except Exception:
        return raw_url

    if parsed.password is None:
        return str(parsed)

    return str(parsed.set(password="***"))


def get_database_debug_info() -> dict[str, object]:
    raw_env_url = os.getenv("DATABASE_URL")
    using_fallback = not bool(raw_env_url)

    try:
        parsed_engine_url = make_url(DATABASE_URL)
        engine_backend = parsed_engine_url.get_backend_name()
        engine_driver = parsed_engine_url.get_driver_name()
        engine_host = parsed_engine_url.host
        engine_database = parsed_engine_url.database
    except Exception:
        engine_backend = None
        engine_driver = None
        engine_host = None
        engine_database = None

    return {
        "database_url_source": "fallback_default" if using_fallback else "environment",
        "raw_env_database_url": redact_database_url(raw_env_url) if raw_env_url else None,
        "normalized_database_url": redact_database_url(DATABASE_URL),
        "engine_database_url": redact_database_url(str(engine.url)),
        "env_matches_engine": normalize_database_url(raw_env_url) == DATABASE_URL if raw_env_url else False,
        "engine_backend": engine_backend,
        "engine_driver": engine_driver,
        "engine_host": engine_host,
        "engine_database": engine_database,
        "is_supabase": bool(engine_host and "supabase.com" in engine_host),
    }


class Base(DeclarativeBase):
    pass


engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def initialize_database() -> None:
    """Create tables and ensure light schema additions exist."""
    debug_info = get_database_debug_info()
    print("[db] runtime database configuration")
    print(f"[db] source={debug_info['database_url_source']}")
    print(f"[db] raw_env={debug_info['raw_env_database_url']}")
    print(f"[db] normalized={debug_info['normalized_database_url']}")
    print(f"[db] engine={debug_info['engine_database_url']}")
    print(
        "[db] backend="
        f"{debug_info['engine_backend']} driver={debug_info['engine_driver']} "
        f"host={debug_info['engine_host']} database={debug_info['engine_database']}"
    )
    print(
        f"[db] env_matches_engine={debug_info['env_matches_engine']} "
        f"is_supabase={debug_info['is_supabase']}"
    )
    Base.metadata.create_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(
            text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS payload JSON NULL")
        )
        connection.execute(
            text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS progress INTEGER DEFAULT 0")
        )
        connection.execute(
            text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS result JSON NULL")
        )
        connection.execute(
            text("ALTER TABLE nodes ADD COLUMN IF NOT EXISTS current_job_id VARCHAR(255) NULL")
        )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
