"""
Data loading utilities for capacity matching and risk flagging.

Handles pattern matching for session capacities and risk flag calculation.
"""

import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def get_capacity(session_name: str, system_config_df: pd.DataFrame) -> Optional[float]:
    """
    Get capacity for a session by pattern matching session_name to match_pattern.
    
    Pattern matching rules:
    - If session_name contains "PERFORM" → match to config where match_pattern contains "PERFORM"
    - If session_name contains "BOX" → match to config where match_pattern contains "BOX"
    - If session_name contains "VO2" → match to config where match_pattern contains "VO2"
    - Same pattern for future additions
    
    Args:
        session_name: Name of the session to get capacity for
        system_config_df: DataFrame with config_key, capacity, match_pattern columns
        
    Returns:
        Capacity value if match found, None otherwise
    """
    if system_config_df.empty:
        return None
    
    # Search for matching pattern (prioritize longer patterns first)
    # Sort by pattern length descending to match longer patterns first
    config_sorted = system_config_df.copy()
    config_sorted["pattern_length"] = config_sorted["match_pattern"].astype(str).str.len()
    config_sorted = config_sorted.sort_values("pattern_length", ascending=False)
    
    session_upper = str(session_name).upper()
    
    for _, config in config_sorted.iterrows():
        match_pattern = str(config.get("match_pattern", "")).upper().strip()
        
        if not match_pattern or match_pattern == "NAN":
            continue
        
        if match_pattern in session_upper:
            capacity = config.get("capacity")
            # Only return if capacity is available
            if pd.notna(capacity) and capacity > 0:
                logger.debug(f"Matched '{session_name}' to pattern '{match_pattern}': capacity={capacity}")
                return float(capacity)
    
    logger.debug(f"No capacity match found for session: {session_name}")
    return None


def calculate_risk_flag(predicted_attendance: float, capacity: Optional[float]) -> str:
    """
    Calculate risk flag based on predicted attendance and capacity.
    
    Risk flag rules:
    - black: capacity data missing (capacity is None)
    - green: utilisation < 0.80
    - amber: utilisation 0.80 - 0.95
    - red: utilisation > 0.95
    
    Args:
        predicted_attendance: Predicted number of attendees
        capacity: Session capacity (None if unavailable)
        
    Returns:
        Risk flag string: 'green', 'amber', 'red', or 'black'
    """
    if capacity is None or capacity <= 0:
        return "black"
    
    utilisation = predicted_attendance / capacity
    
    if utilisation < 0.80:
        return "green"
    elif utilisation <= 0.95:
        return "amber"
    else:
        return "red"


def prepare_forecast_output(
    forecasts_df: pd.DataFrame,
    system_config_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Prepare forecast DataFrame for database insertion.
    
    Adds predicted_utilisation and risk_flag columns based on capacity matching.
    
    Args:
        forecasts_df: DataFrame with predicted_attendance and session information
        system_config_df: DataFrame with system configuration for capacity matching
        
    Returns:
        DataFrame ready for database insertion with columns:
        - session_date
        - session_name
        - session_start
        - session_end (if available)
        - predicted_attendance
        - predicted_utilisation (nullable)
        - risk_flag
    """
    df = forecasts_df.copy()
    
    if "predicted_attendance" not in df.columns:
        raise ValueError("forecasts_df must have predicted_attendance column")
    
    # Get capacity for each session
    df["capacity"] = df["session_name"].apply(
        lambda name: get_capacity(name, system_config_df)
    )
    
    # Calculate utilisation
    df["predicted_utilisation"] = df.apply(
        lambda row: row["predicted_attendance"] / row["capacity"] 
        if pd.notna(row["capacity"]) and row["capacity"] > 0 
        else None,
        axis=1
    )
    
    # Calculate risk flag
    df["risk_flag"] = df.apply(
        lambda row: calculate_risk_flag(row["predicted_attendance"], row.get("capacity")),
        axis=1
    )
    
    # Select output columns
    output_cols = [
        "session_date",
        "session_name",
        "session_start",
        "predicted_attendance",
        "predicted_utilisation",
        "risk_flag"
    ]
    
    # Include session_end if present
    if "session_end" in df.columns:
        output_cols.insert(3, "session_end")
    
    output_df = df[output_cols].copy()
    
    # Log statistics
    risk_counts = output_df["risk_flag"].value_counts()
    logger.info("Risk flag distribution:")
    for flag, count in risk_counts.items():
        logger.info(f"  {flag}: {count}")
    
    logger.info(f"Forecasts with capacity data: {(output_df['predicted_utilisation'].notna().sum())}/{len(output_df)}")
    
    return output_df
