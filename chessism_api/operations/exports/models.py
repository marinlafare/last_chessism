"""Small typed integration records. Requests/manifests are files, not JSON DB columns."""
from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ExportClient(Base):
    __tablename__ = 'export_client'
    __table_args__ = {'schema': 'public'}
    id = Column(String(32), primary_key=True)
    name = Column(String(100), nullable=False)
    token_hash = Column(String(64), nullable=False, unique=True)
    allowed_sources = Column(ARRAY(String(40)), nullable=False)
    max_rows = Column(Integer, nullable=False)
    allow_analytics = Column(Boolean, nullable=False, default=False, server_default='false')
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)
    revoked_at = Column(DateTime(timezone=True))


class ExportJob(Base):
    __tablename__ = 'export_job'
    __table_args__ = (UniqueConstraint('client_id', 'idempotency_key'), {'schema': 'public'})
    __mapper_args__ = {'eager_defaults': True}
    id = Column(String(32), primary_key=True)
    client_id = Column(String(32), ForeignKey('public.export_client.id'), nullable=False, index=True)
    idempotency_key = Column(String(128), nullable=False)
    request_sha256 = Column(String(64), nullable=False)
    name = Column(String(100), nullable=False)
    source = Column(String(40), nullable=False)
    row_limit = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default='queued', index=True)
    phase = Column(String(32), nullable=False, default='queued')
    rows_written = Column(BigInteger, nullable=False, default=0)
    file_bytes = Column(BigInteger, nullable=False, default=0)
    file_sha256 = Column(String(64))
    limit_reached = Column(Boolean)
    cancel_requested = Column(Boolean, nullable=False, default=False)
    error = Column(String(500))
    error_code = Column(String(64))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime(timezone=True))
    snapshot_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    expires_at = Column(DateTime(timezone=True), index=True)
