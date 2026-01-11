"""
Forecast generation for next 6 weeks (42 days).

Creates forecast grid, applies member holds adjustment, and prepares output.
"""

import logging
from datetime import date, timedelta
from typing import List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def get_session_slots(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique session slot combinations from historical data.
    
    A session slot is defined by (session_name, session_start, session_end).
    
    Args:
        df: Historical session-level DataFrame
        
    Returns:
        DataFrame with unique session slots (columns: session_name, session_start, session_end)
    """
    slot_cols = ["session_name", "session_start"]
    if "session_end" in df.columns:
        slot_cols.append("session_end")
    
    slots = df[slot_cols].drop_duplicates().reset_index(drop=True)
    
    logger.info(f"Identified {len(slots)} unique session slots")
    return slots


def generate_forecast_dates(
    start_date: date,
    horizon_days: int = 14,
    work_calendar: pd.DataFrame = None
) -> List[date]:
    """
    Generate forecast dates for the next N days.
    
    Filters out non-business days if work_calendar is provided.
    
    Args:
        start_date: Starting date (typically tomorrow or next business day)
        horizon_days: Number of days to forecast
        work_calendar: Optional DataFrame with business day information
        
    Returns:
        List of forecast dates (business days only if calendar provided)
    """
    # Generate all dates
    dates = [start_date + timedelta(days=i) for i in range(horizon_days)]
    
    # Filter to business days if calendar provided
    if work_calendar is not None and "is_business_day" in work_calendar.columns:
        work_cal = work_calendar.copy()
        work_cal["the_date"] = pd.to_datetime(work_cal["the_date"]).dt.date
        
        # Only include business days
        business_days = set(
            work_cal[work_cal["is_business_day"] == True]["the_date"].tolist()
        )
        dates = [d for d in dates if d in business_days]
        
        logger.info(f"Filtered to {len(dates)} business days out of {horizon_days} days")
    
    return dates


def build_forecast_features(
    session_slots: pd.DataFrame,
    forecast_dates: List[date],
    historical_df: pd.DataFrame,
    work_calendar: pd.DataFrame
) -> pd.DataFrame:
    """
    Build feature matrix for forecast dates.
    
    For each (session_slot × forecast_date) combination:
    - Use most recent historical values for lag/rolling features
    - Add temporal features (day_of_week, week_of_year)
    - Add holiday flags
    
    Args:
        session_slots: DataFrame with unique session slots
        forecast_dates: List of dates to forecast
        historical_df: Historical session-level data with features
        work_calendar: Work calendar for holiday flags
        
    Returns:
        DataFrame with forecast grid and features
    """
    from src.feature_engineering import add_temporal_features, add_holiday_feature
    
    # Create forecast grid: cartesian product of slots × dates
    forecast_grid = []
    
    for _, slot in session_slots.iterrows():
        for forecast_date in forecast_dates:
            row = {
                "session_date": forecast_date,
                "session_name": slot["session_name"],
                "session_start": slot["session_start"]
            }
            if "session_end" in slot:
                row["session_end"] = slot["session_end"]
            forecast_grid.append(row)
    
    forecast_df = pd.DataFrame(forecast_grid)
    
    logger.info(f"Created forecast grid: {len(forecast_df)} rows ({len(session_slots)} slots × {len(forecast_dates)} dates)")
    
    # Add temporal features
    forecast_df = add_temporal_features(forecast_df)
    
    # Add holiday features
    forecast_df = add_holiday_feature(forecast_df, work_calendar)
    
    # For each session slot, get most recent historical features
    # Sort historical data by date
    historical_df = historical_df.copy()
    historical_df["session_date"] = pd.to_datetime(historical_df["session_date"])
    historical_df = historical_df.sort_values("session_date")
    
    # Get latest values per session slot for lag/rolling features
    slot_identifier = (
        historical_df["session_name"].astype(str) + "_" + 
        historical_df["session_start"].astype(str)
    )
    historical_df["session_slot"] = slot_identifier
    
    # Get most recent row per slot (contains latest lag/rolling values)
    latest_per_slot = historical_df.groupby("session_slot").last().reset_index()
    
    # Create slot identifier for forecast grid
    forecast_df["session_slot"] = (
        forecast_df["session_name"].astype(str) + "_" + 
        forecast_df["session_start"].astype(str)
    )
    
    # Merge latest features
    feature_cols = ["lag_1_attendance", "rolling_avg_4", "rolling_avg_8"]
    available_features = [col for col in feature_cols if col in latest_per_slot.columns]
    
    if available_features:
        merge_cols = ["session_slot"] + available_features
        forecast_df = forecast_df.merge(
            latest_per_slot[merge_cols],
            on="session_slot",
            how="left"
        )
    
    # Fill missing values (slots with no history) with 0
    for col in feature_cols:
        if col in forecast_df.columns:
            forecast_df[col] = forecast_df[col].fillna(0)
    
    # Drop helper column
    forecast_df = forecast_df.drop(columns=["session_slot"])
    
    # Convert session_date back to date
    forecast_df["session_date"] = pd.to_datetime(forecast_df["session_date"]).dt.date
    
    logger.info("Forecast features built successfully")
    return forecast_df


def apply_member_holds_adjustment(
    forecasts_df: pd.DataFrame,
    member_holds_df: pd.DataFrame,
    forecast_dates: List[date],
    historical_attendance_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Apply smart member holds adjustment to forecasts.
    
    For each member on hold:
    1. Calculate their attendance pattern from historical data
    2. For days within hold period, subtract their expected attendance from their usual sessions only
    3. For days after hold ends, include their attendance normally
    
    Excludes holds with NULL end dates completely.
    
    Args:
        forecasts_df: DataFrame with predicted_attendance column
        member_holds_df: DataFrame with member_id, hold_start, hold_end
        forecast_dates: List of forecast dates
        historical_attendance_df: DataFrame with member-level historical attendance data
        
    Returns:
        DataFrame with adjusted predicted_attendance
    """
    forecasts_df = forecasts_df.copy()
    
    if "predicted_attendance" not in forecasts_df.columns:
        raise ValueError("forecasts_df must have predicted_attendance column")
    
    if member_holds_df.empty:
        logger.info("No member holds data, skipping adjustment")
        return forecasts_df
    
    # Step 1: Filter out holds with NULL end dates completely
    member_holds_df = member_holds_df.copy()
    member_holds_df = member_holds_df[member_holds_df["hold_end"].notna()].copy()
    
    if member_holds_df.empty:
        logger.info("No holds with end dates found, skipping adjustment")
        return forecasts_df
    
    logger.info(f"Processing {len(member_holds_df)} holds with end dates")
    
    # Step 2: Calculate member attendance patterns from historical data
    # Group by member_id and session slot to calculate attendance rate
    historical_attendance_df = historical_attendance_df.copy()
    
    # Create session slot identifier
    historical_attendance_df["session_slot"] = (
        historical_attendance_df["session_name"].astype(str) + "_" + 
        historical_attendance_df["session_start"].astype(str)
    )
    
    # Calculate each member's attendance rate per session slot
    # This is the probability that a member attends a specific session
    member_patterns = []
    
    # Get all unique session slots from historical data
    all_session_slots = historical_attendance_df["session_slot"].unique()
    
    # Calculate total occurrences of each session slot (how many times each session ran)
    session_occurrence_counts = historical_attendance_df.groupby("session_slot")["session_date"].nunique()
    
    for member_id in member_holds_df["member_id"].unique():
        member_attendance = historical_attendance_df[
            historical_attendance_df["member_id"] == member_id
        ]
        
        if member_attendance.empty:
            # Member has no historical data, skip
            continue
        
        # Count how many times member attended each session slot
        member_attendance_by_slot = member_attendance.groupby("session_slot").size()
        
        # Calculate attendance rate per session slot
        for session_slot in all_session_slots:
            times_attended = member_attendance_by_slot.get(session_slot, 0)
            times_session_ran = session_occurrence_counts.get(session_slot, 1)
            
            if times_session_ran == 0:
                continue
            
            # Attendance rate: how often member attends this session when it runs
            attendance_rate = times_attended / times_session_ran
            
            # Only include sessions member actually attends (rate > 0)
            if attendance_rate > 0:
                member_patterns.append({
                    "member_id": member_id,
                    "session_slot": session_slot,
                    "attendance_rate": min(attendance_rate, 1.0)  # Cap at 1.0
                })
    
    if not member_patterns:
        logger.warning("No member attendance patterns found, skipping adjustment")
        return forecasts_df
    
    member_patterns_df = pd.DataFrame(member_patterns)
    logger.info(f"Calculated attendance patterns for {member_patterns_df['member_id'].nunique()} members")
    
    # Step 3: Create session slot identifier in forecasts_df
    forecasts_df["session_slot"] = (
        forecasts_df["session_name"].astype(str) + "_" + 
        forecasts_df["session_start"].astype(str)
    )
    
    # Step 4: For each forecast date and session, calculate adjustment
    adjustment_dict = {}  # (date, session_slot) -> adjustment amount
    
    for forecast_date in forecast_dates:
        # Find members on hold for this date
        on_hold = member_holds_df[
            (member_holds_df["hold_start"] <= forecast_date) &
            (member_holds_df["hold_end"] >= forecast_date)
        ]
        
        if on_hold.empty:
            continue
        
        # For each session slot, calculate total adjustment from held members
        for session_slot in forecasts_df["session_slot"].unique():
            # Find held members who typically attend this session
            held_members = on_hold["member_id"].unique()
            patterns_for_slot = member_patterns_df[
                (member_patterns_df["member_id"].isin(held_members)) &
                (member_patterns_df["session_slot"] == session_slot)
            ]
            
            # Sum up the expected attendance from held members for this session
            adjustment = patterns_for_slot["attendance_rate"].sum()
            
            if adjustment > 0:
                key = (forecast_date, session_slot)
                adjustment_dict[key] = adjustment
    
    logger.info(f"Calculated adjustments for {len(adjustment_dict)} date-session combinations")
    
    # Step 5: Apply adjustments
    def adjust_attendance(row):
        date_val = row["session_date"]
        session_slot = row["session_slot"]
        key = (date_val, session_slot)
        
        adjustment = adjustment_dict.get(key, 0.0)
        adjusted = max(0, row["predicted_attendance"] - adjustment)  # Can't go negative
        return adjusted
    
    forecasts_df["predicted_attendance"] = forecasts_df.apply(adjust_attendance, axis=1)
    
    # Clean up temporary column
    forecasts_df = forecasts_df.drop(columns=["session_slot"])
    
    total_adjustment = sum(adjustment_dict.values())
    logger.info(f"Applied smart member holds adjustment: {total_adjustment:.2f} total expected attendance removed")
    
    return forecasts_df
