from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sentinelq:sentinelq@localhost:5432/sentinelq")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class JobRecord(Base):
    __tablename__ = "jobs"

    id           = Column(String, primary_key=True)
    alert_name   = Column(String, nullable=False)
    severity     = Column(String, nullable=False)
    priority     = Column(String, nullable=False)
    source       = Column(String, nullable=False)
    payload      = Column(JSON, default={})
    status       = Column(String, nullable=False, default="queued")
    retry_count  = Column(Integer, default=0)
    max_retries  = Column(Integer, default=3)
    worker_id    = Column(String, nullable=True)
    error        = Column(Text, nullable=True)
    deduplicated = Column(Boolean, default=False)
    created_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ScheduledAlert(Base):
    __tablename__ = "scheduled_alerts"

    id          = Column(String, primary_key=True)
    alert_name  = Column(String, nullable=False)
    severity    = Column(String, nullable=False)
    source      = Column(String, nullable=False, default="scheduler")
    payload     = Column(JSON, default={})
    cron_expr   = Column(String, nullable=False)
    is_active   = Column(Boolean, default=True)
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)


def init_db() -> None:
    try:
        Base.metadata.create_all(bind=engine)
    except Exception:
        # Another process won the race to CREATE TABLE — already exists, ignore
        pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
