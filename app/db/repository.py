import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

from services.distance_service import DistanceMode, GeocodingProvider, RoutingProvider

load_dotenv()


class Base(DeclarativeBase):
    pass


class DistanceRequest(Base):
    __tablename__ = "distance_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    filename: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    total_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="created")

    results: Mapped[list["DistanceResult"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )


class DistanceResult(Base):
    __tablename__ = "distance_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("distance_requests.id", ondelete="CASCADE"),
        nullable=True,
    )
    origin_raw: Mapped[str] = mapped_column(Text, nullable=False)
    destination_raw: Mapped[str] = mapped_column(Text, nullable=False)
    origin_lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    origin_lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    destination_lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    destination_lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    distance_km: Mapped[Optional[float]] = mapped_column(Numeric(12, 3), nullable=True)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    geocoding_provider: Mapped[str] = mapped_column(String(32), nullable=False, default="AUTO")
    routing_provider: Mapped[str] = mapped_column(String(32), nullable=False, default="AUTO")
    geocoding_provider_used: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    routing_provider_used: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    fallback_used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ok")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    request: Mapped[Optional[DistanceRequest]] = relationship(back_populates="results")


DATABASE_URL = os.getenv("DATABASE_URL") or "sqlite:///local.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_distance_result_columns()


def _ensure_distance_result_columns() -> None:
    required_columns = {
        "geocoding_provider": "VARCHAR(32) NOT NULL DEFAULT 'AUTO'",
        "routing_provider": "VARCHAR(32) NOT NULL DEFAULT 'AUTO'",
        "geocoding_provider_used": "VARCHAR(32)",
        "routing_provider_used": "VARCHAR(32)",
        "fallback_used": "BOOLEAN NOT NULL DEFAULT FALSE",
    }

    with engine.begin() as connection:
        inspector = inspect(connection)
        existing = {column["name"] for column in inspector.get_columns("distance_results")}
        for column_name, ddl in required_columns.items():
            if column_name in existing:
                continue
            connection.execute(text(f"ALTER TABLE distance_results ADD COLUMN {column_name} {ddl}"))


def create_distance_request(filename: str, mode: DistanceMode, total_rows: int) -> DistanceRequest:
    session = SessionLocal()
    try:
        obj = DistanceRequest(
            filename=filename,
            mode=mode.name,
            total_rows=total_rows,
            processed_rows=0,
            status="created",
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj
    finally:
        session.close()


def update_distance_request_progress(request_id: int, processed_rows: int, status: str) -> None:
    session = SessionLocal()
    try:
        stmt = select(DistanceRequest).where(DistanceRequest.id == request_id)
        obj = session.scalars(stmt).first()
        if not obj:
            return
        obj.processed_rows = processed_rows
        obj.status = status
        session.add(obj)
        session.commit()
    finally:
        session.close()


def save_distance_result(
    request_id: Optional[int],
    origin_raw: str,
    destination_raw: str,
    origin_lat: Optional[float],
    origin_lng: Optional[float],
    destination_lat: Optional[float],
    destination_lng: Optional[float],
    distance_km: Optional[float],
    mode: DistanceMode,
    geocoding_provider: GeocodingProvider,
    routing_provider: RoutingProvider,
    status: str,
    error_message: Optional[str],
    geocoding_provider_used: Optional[str],
    routing_provider_used: Optional[str],
    fallback_used: bool,
) -> DistanceResult:
    session = SessionLocal()
    try:
        obj = DistanceResult(
            request_id=request_id,
            origin_raw=origin_raw,
            destination_raw=destination_raw,
            origin_lat=origin_lat,
            origin_lng=origin_lng,
            destination_lat=destination_lat,
            destination_lng=destination_lng,
            distance_km=distance_km,
            mode=mode.name,
            geocoding_provider=geocoding_provider.name,
            routing_provider=routing_provider.name,
            geocoding_provider_used=geocoding_provider_used,
            routing_provider_used=routing_provider_used,
            fallback_used=fallback_used,
            status=status,
            error_message=error_message,
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj
    finally:
        session.close()


def get_cached_distance(
    origin: str,
    destination: str,
    mode: DistanceMode,
    geocoding_provider: GeocodingProvider,
    routing_provider: RoutingProvider,
) -> Optional[float]:
    session = SessionLocal()
    try:
        stmt = (
            select(DistanceResult)
            .where(DistanceResult.origin_raw == origin)
            .where(DistanceResult.destination_raw == destination)
            .where(DistanceResult.mode == mode.name)
            .where(DistanceResult.geocoding_provider == geocoding_provider.name)
            .where(DistanceResult.routing_provider == routing_provider.name)
            .where(DistanceResult.status == "ok")
        )
        obj = session.scalars(stmt).first()
        if not obj or obj.distance_km is None:
            return None
        return float(obj.distance_km)
    finally:
        session.close()

