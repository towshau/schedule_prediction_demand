"""
Feature engineering for the forecasting model.

Creates temporal features, holiday flags, and lag/rolling features
while preventing data leakage.
"""

import logging
from typing import Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add temporal features: day_of_week and week_of_year.
    
    Args:
        df: DataFrame with session_date column
        
    Returns:
        DataFrame with added temporal features
    """
    df = df.copy()
    
    if "session_date" not in df.columns:
        raise ValueError("DataFrame must have session_date column")
    
    # Ensure session_date is datetime
    df["session_date"] = pd.to_datetime(df["session_date"])
    
    # Add day of week (Monday=0, Sunday=6)
    df["day_of_week"] = df["session_date"].dt.dayofweek
    
    # Add week of year
    df["week_of_year"] = df["session_date"].dt.isocalendar().week
    
    # Convert back to date if it was originally date
    if df["session_date"].dtype != "datetime64[ns]":
        df["session_date"] = df["session_date"].dt.date
    
    logger.info("Added temporal features: day_of_week, week_of_year")
    return df


def add_holiday_feature(df: pd.DataFrame, work_calendar: pd.DataFrame) -> pd.DataFrame:
    """
    Add is_holiday feature by joining to work_calendar.
    
    A day is considered a holiday if is_business_day = false OR holiday_name IS NOT NULL.
    
    Args:
        df: DataFrame with session_date column
        work_calendar: DataFrame with the_date, is_business_day, holiday_name columns
        
    Returns:
        DataFrame with added is_holiday column (boolean)
    """
    df = df.copy()
    work_cal = work_calendar.copy()
    
    if "session_date" not in df.columns:
        raise ValueError("DataFrame must have session_date column")
    
    # Ensure dates are comparable
    df["session_date"] = pd.to_datetime(df["session_date"]).dt.date
    work_cal["the_date"] = pd.to_datetime(work_cal["the_date"]).dt.date
    
    # Create is_holiday flag in calendar
    work_cal["is_holiday"] = (
        (work_cal["is_business_day"] == False) | 
        (work_cal["holiday_name"].notna())
    )
    
    # Join to add holiday flag
    df = df.merge(
        work_cal[["the_date", "is_holiday"]],
        left_on="session_date",
        right_on="the_date",
        how="left"
    )
    
    # Fill missing values (dates not in calendar) as False (assume not holiday)
    df["is_holiday"] = df["is_holiday"].fillna(False)
    
    # Drop the_date column from merge
    df = df.drop(columns=["the_date"], errors="ignore")
    
    logger.info(f"Added holiday feature: {df['is_holiday'].sum()} holidays identified")
    return df


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add lag and rolling average features per session slot.
    
    For each unique (session_name, session_start) slot:
    - lag_1_attendance: Previous occurrence's attendance
    - rolling_avg_4: Rolling average of last 4 occurrences (excluding current)
    - rolling_avg_8: Rolling average of last 8 occurrences (excluding current)
    
    CRITICAL: Uses only prior data to prevent leakage.
    
    Args:
        df: DataFrame with session_date, session_name, session_start, actual_attendance
        
    Returns:
        DataFrame with added lag features
    """
    df = df.copy()
    
    required_cols = ["session_date", "session_name", "session_start", "actual_attendance"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for lag features: {', '.join(missing)}")
    
    # Ensure session_date is datetime for sorting
    df["session_date"] = pd.to_datetime(df["session_date"])
    
    # Sort by session slot and date to ensure proper ordering
    df = df.sort_values(["session_name", "session_start", "session_date"]).reset_index(drop=True)
    
    # Create session slot identifier
    df["session_slot"] = df["session_name"] + "_" + df["session_start"].astype(str)
    
    # Group by session slot and compute lag features
    def compute_lag_features(group):
        """Compute lag features for a single session slot."""
        group = group.sort_values("session_date").copy()
        
        # Lag-1: previous occurrence's attendance
        group["lag_1_attendance"] = group["actual_attendance"].shift(1)
        
        # Rolling averages using only prior data (shift to exclude current row)
        group["rolling_avg_4"] = (
            group["actual_attendance"]
            .shift(1)  # Exclude current row
            .rolling(window=4, min_periods=1)
            .mean()
        )
        
        group["rolling_avg_8"] = (
            group["actual_attendance"]
            .shift(1)  # Exclude current row
            .rolling(window=8, min_periods=1)
            .mean()
        )
        
        return group
    
    # Apply to each session slot
    df = df.groupby("session_slot", group_keys=False).apply(compute_lag_features)
    
    # Drop session_slot helper column
    df = df.drop(columns=["session_slot"])
    
    # Convert session_date back to date if needed
    df["session_date"] = df["session_date"].dt.date
    
    logger.info("Added lag features: lag_1_attendance, rolling_avg_4, rolling_avg_8")
    
    # Log feature statistics
    logger.info(f"Lag-1 coverage: {df['lag_1_attendance'].notna().sum()}/{len(df)} rows")
    logger.info(f"Rolling avg-4 coverage: {df['rolling_avg_4'].notna().sum()}/{len(df)} rows")
    logger.info(f"Rolling avg-8 coverage: {df['rolling_avg_8'].notna().sum()}/{len(df)} rows")
    
    return df


def build_feature_matrix(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Build feature matrix X and target vector y from DataFrame.
    
    Args:
        df: DataFrame with all features and optionally actual_attendance target
        
    Returns:
        Tuple of (X: feature matrix, y: target vector or empty Series if no target)
    """
    # Feature columns (exclude non-feature columns)
    exclude_cols = [
        "session_date", "session_name", "session_start", "session_end",
        "actual_attendance", "predicted_attendance", "the_date", "session_slot",
        "capacity"  # capacity is used for calculation, not a feature
    ]
    
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    if not feature_cols:
        raise ValueError("No feature columns found in DataFrame")
    
    X = df[feature_cols].copy()
    
    # Only extract target if it exists (for training data, not forecast data)
    if "actual_attendance" in df.columns:
        y = df["actual_attendance"].copy()
    else:
        y = pd.Series(dtype=float)  # Empty series for forecast data
    
    # Fill any remaining NaN values in features (e.g., lag features for first occurrences)
    X = X.fillna(0)
    
    logger.info(f"Built feature matrix: {len(X)} samples, {len(feature_cols)} features")
    logger.info(f"Features: {', '.join(feature_cols)}")
    
    return X, y
