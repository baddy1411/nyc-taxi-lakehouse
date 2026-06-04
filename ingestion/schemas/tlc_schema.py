"""
ingestion/schemas/tlc_schema.py
────────────────────────────────
Pydantic schema enforcing the NYC TLC Yellow Taxi data contract.

This is the TRUST BOUNDARY — every row that crosses from raw source
into the Bronze layer must pass this contract. Failures are quarantined,
never silently dropped or coerced.

Schema version: 2024-01 (matches TLC data dictionary revision Jan 2024)
Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""
from __future__ import annotations

from datetime import datetime
from enum import IntEnum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class VendorID(IntEnum):
    CREATIVE_MOBILE = 1
    VERIFONE        = 2


class RateCode(IntEnum):
    STANDARD    = 1
    JFK         = 2
    NEWARK      = 3
    NASSAU      = 4
    NEGOTIATED  = 5
    GROUP_RIDE  = 6


class PaymentType(IntEnum):
    CREDIT_CARD = 1
    CASH        = 2
    NO_CHARGE   = 3
    DISPUTE     = 4
    UNKNOWN     = 5
    VOIDED      = 6


class TLCTripRecord(BaseModel):
    """
    One NYC Yellow Taxi trip record.
    All field constraints sourced from the TLC data dictionary.
    """

    # --- Identity ----------------------------------------------------------
    vendorid:             int         = Field(ge=1, le=2)
    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime

    # --- Trip attributes ---------------------------------------------------
    passenger_count:  Optional[int]   = Field(default=None, ge=0, le=9)
    trip_distance:    float           = Field(ge=0.0, le=500.0)
    ratecodeid:       Optional[int]   = Field(default=None, ge=1, le=6)
    store_and_fwd_flag: Optional[str] = None
    pulocationid:     int             = Field(ge=1, le=265)
    dolocationid:     int             = Field(ge=1, le=265)
    payment_type:     int             = Field(ge=1, le=6)

    # --- Fares -------------------------------------------------------------
    fare_amount:            float = Field(ge=-10.0)   # small negatives = adjustments
    extra:                  float = Field(ge=-5.0, le=20.0)
    mta_tax:                float
    tip_amount:             float = Field(ge=0.0)
    tolls_amount:           float = Field(ge=0.0)
    improvement_surcharge:  float
    total_amount:           float
    congestion_surcharge:   Optional[float] = Field(default=0.0, ge=0.0, le=5.0)
    airport_fee:            Optional[float] = Field(default=0.0, ge=0.0, le=10.0)

    # --- Cross-field validators -------------------------------------------

    @field_validator("store_and_fwd_flag")
    @classmethod
    def normalise_flag(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().upper()
        if v not in {"Y", "N"}:
            raise ValueError(f"store_and_fwd_flag must be Y or N, got {v!r}")
        return v

    @model_validator(mode="after")
    def validate_temporal_logic(self) -> "TLCTripRecord":
        """Dropoff must be after pickup; trip can't be longer than 24 hours."""
        if self.tpep_dropoff_datetime <= self.tpep_pickup_datetime:
            raise ValueError(
                f"dropoff ({self.tpep_dropoff_datetime}) must be after "
                f"pickup ({self.tpep_pickup_datetime})"
            )
        duration_hours = (
            self.tpep_dropoff_datetime - self.tpep_pickup_datetime
        ).total_seconds() / 3600
        if duration_hours > 24:
            raise ValueError(f"Trip duration {duration_hours:.1f}h exceeds 24h cap")
        return self

    @model_validator(mode="after")
    def validate_tip_on_cash(self) -> "TLCTripRecord":
        """Cash payments (type 2) should not have recorded tips."""
        if self.payment_type == PaymentType.CASH and self.tip_amount > 0:
            raise ValueError(
                "Cash payments cannot have tip_amount > 0 "
                "(TLC only records CC tips electronically)"
            )
        return self

    @model_validator(mode="after")
    def validate_jfk_fare(self) -> "TLCTripRecord":
        """JFK flat rate (code 2) should be ~$70."""
        if self.ratecodeid == RateCode.JFK:
            if not (60.0 <= self.fare_amount <= 90.0):
                raise ValueError(
                    f"JFK flat rate should be 60–90, got {self.fare_amount}"
                )
        return self


# ---------------------------------------------------------------------------
# Batch validation helper
# ---------------------------------------------------------------------------

import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    valid_df:      pd.DataFrame
    quarantine_df: pd.DataFrame
    error_summary: dict[str, int] = field(default_factory=dict)

    @property
    def pass_rate(self) -> float:
        total = len(self.valid_df) + len(self.quarantine_df)
        return len(self.valid_df) / total if total > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"ValidationResult: {len(self.valid_df):,} valid "
            f"({self.pass_rate:.1%}), "
            f"{len(self.quarantine_df):,} quarantined"
        )


def validate_batch(df: pd.DataFrame) -> ValidationResult:
    """
    Validates a DataFrame of raw TLC records against TLCTripRecord schema.

    Returns valid rows and quarantine rows separately.
    Invalid rows are never silently dropped — they land in quarantine
    with an _error_reason column for audit.
    """
    valid_rows    = []
    quarantine_rows = []
    error_counts: dict[str, int] = {}

    for row in df.itertuples(index=False):
        try:
            TLCTripRecord(**row._asdict())
            valid_rows.append(row._asdict())
        except Exception as e:
            error_msg = str(e)[:120]
            # Classify error type (first field mentioned)
            key = error_msg.split(" ")[0]
            error_counts[key] = error_counts.get(key, 0) + 1
            q = row._asdict()
            q["_error_reason"] = error_msg
            quarantine_rows.append(q)

    valid_df      = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
    quarantine_df = pd.DataFrame(quarantine_rows) if quarantine_rows else pd.DataFrame()

    result = ValidationResult(valid_df, quarantine_df, error_counts)
    logger.info("%s | top errors: %s", result, list(error_counts.items())[:3])
    return result
