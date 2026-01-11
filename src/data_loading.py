"""
Data loading utilities for capacity matching and risk flagging.

Handles pattern matching for session capacities and risk flag calculation.
"""

import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def get_base_capacity(session_name: str, system_config_df: pd.DataFrame) -> Optional[float]:
    """
    Get base capacity for a session by pattern matching session_name to match_pattern.
    
    This returns the base capacity per coach. Multiply by number of coaches to get total capacity.
    
    Pattern matching rules:
    - If session_name contains "PERFORM" → match to config where match_pattern contains "PERFORM"
    - If session_name contains "BOX" → match to config where match_pattern contains "BOX"
    - If session_name contains "VO2" → match to config where match_pattern contains "VO2"
    - Same pattern for future additions
    
    Args:
        session_name: Name of the session to get capacity for
        system_config_df: DataFrame with config_key, capacity, match_pattern columns
        
    Returns:
        Base capacity value if match found, None otherwise
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
                logger.debug(f"Matched '{session_name}' to pattern '{match_pattern}': base capacity={capacity}")
                return float(capacity)
    
    logger.debug(f"No base capacity match found for session: {session_name}")
    return None


def count_coaches_from_string(coach_name: str) -> int:
    """
    Count the number of coaches from a coach_name string.
    
    Coach names can be:
    - Single coach: "Andy Kong"
    - Multiple coaches comma-separated: "Andy Kong, Jarryd Wearne"
    
    Args:
        coach_name: Coach name string (can be comma-separated)
        
    Returns:
        Number of coaches (1 if single, count if comma-separated)
    """
    if pd.isna(coach_name) or not coach_name or str(coach_name).strip() == "":
        return 1  # Default to 1 coach if missing
    
    coach_str = str(coach_name).strip()
    
    # If comma-separated, split and count
    if ',' in coach_str:
        coaches = [c.strip() for c in coach_str.split(',') if c.strip()]
        return len(coaches)
    
    # Single coach
    return 1


def calculate_coach_counts(attendance_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate typical number of coaches per session slot from historical data.
    
    For each unique session slot (session_name + session_start), calculates
    the typical number of coaches based on historical attendance data.
    
    Args:
        attendance_df: DataFrame with member-level attendance data
            Required columns: session_name, session_start, coach_name
        
    Returns:
        DataFrame with columns: session_name, session_start, coach_count
        One row per unique session slot with typical coach count
    """
    if "coach_name" not in attendance_df.columns:
        logger.warning("coach_name column not found, defaulting to 1 coach per session")
        # Return default coach count of 1 for all session slots
        unique_slots = attendance_df[["session_name", "session_start"]].drop_duplicates()
        unique_slots["coach_count"] = 1
        return unique_slots[["session_name", "session_start", "coach_count"]]
    
    df = attendance_df.copy()
    
    # Normalize session_start to comparable format (HH:MM:SS or HH:MM)
    # Convert to string format HH:MM for consistent matching
    df["session_start_str"] = pd.to_datetime(df["session_start"], format="%H:%M:%S", errors="coerce").dt.strftime("%H:%M")
    # Fill any that didn't parse as time with string conversion
    df["session_start_str"] = df["session_start_str"].fillna(df["session_start"].astype(str).str[:5])
    
    # For each session occurrence, count coaches (parse comma-separated names)
    df["coach_count"] = df["coach_name"].apply(count_coaches_from_string)
    
    # Calculate typical coach count per session slot
    # Use median to handle variations (some sessions might have different coach counts on different days)
    coach_counts = (
        df.groupby(["session_name", "session_start_str"])["coach_count"]
        .median()
        .round()
        .astype(int)
        .reset_index()
    )
    
    # Rename back to session_start for consistency
    coach_counts = coach_counts.rename(columns={"session_start_str": "session_start"})
    
    # Ensure minimum of 1 coach
    coach_counts["coach_count"] = coach_counts["coach_count"].clip(lower=1)
    
    logger.info(f"Calculated coach counts for {len(coach_counts)} session slots")
    logger.info(f"Coach count range: {coach_counts['coach_count'].min()} to {coach_counts['coach_count'].max()}")
    
    return coach_counts


def get_capacity(
    session_name: str,
    session_start: str,
    system_config_df: pd.DataFrame,
    coach_counts_df: pd.DataFrame = None,
    num_coaches: int = None
) -> Optional[float]:
    """
    Get total capacity for a session by multiplying base capacity by number of coaches.
    
    Formula: Total Capacity = Base Capacity × Number of Coaches
    
    For example:
    - Base capacity for PERFORM = 2.0
    - Number of coaches = 2 (Andy Kong, Jarryd Wearne)
    - Total capacity = 2.0 × 2 = 4.0
    
    Args:
        session_name: Name of the session
        session_start: Start time of the session (to match with coach_counts_df)
        system_config_df: DataFrame with base capacity settings
        coach_counts_df: Optional DataFrame with coach counts per session slot
        num_coaches: Optional direct number of coaches (overrides coach_counts_df lookup)
        
    Returns:
        Total capacity (base capacity × coaches) if match found, None otherwise
    """
    # Get base capacity from system_config
    base_capacity = get_base_capacity(session_name, system_config_df)
    
    if base_capacity is None:
        return None
    
    # Get number of coaches
    if num_coaches is not None:
        coach_count = int(num_coaches)
    elif coach_counts_df is not None:
        # Convert session_start to comparable format
        session_start_str = str(session_start)[:5] if hasattr(session_start, '__str__') else str(session_start)
        
        # Look up coach count for this session slot
        # Compare by converting both to string format HH:MM
        match = coach_counts_df[
            (coach_counts_df["session_name"] == session_name) &
            (coach_counts_df["session_start"].astype(str).str[:5] == session_start_str[:5])
        ]
        
        if not match.empty:
            coach_count = int(match.iloc[0]["coach_count"])
        else:
            logger.debug(f"No coach count found for {session_name} @ {session_start_str}, defaulting to 1")
            coach_count = 1  # Default to 1 coach if not found
    else:
        coach_count = 1  # Default to 1 coach if no coach data provided
    
    # Calculate total capacity
    total_capacity = base_capacity * coach_count
    
    logger.debug(
        f"Capacity for {session_name} @ {session_start}: "
        f"base={base_capacity} × coaches={coach_count} = {total_capacity}"
    )
    
    return total_capacity


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
    system_config_df: pd.DataFrame,
    attendance_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Prepare forecast DataFrame for database insertion.
    
    Adds predicted_utilisation and risk_flag columns based on capacity matching.
    Capacity is calculated as: Base Capacity × Number of Coaches
    
    Args:
        forecasts_df: DataFrame with predicted_attendance and session information
        system_config_df: DataFrame with system configuration for capacity matching
        attendance_df: Optional DataFrame with historical attendance data to calculate coach counts
        
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
    
    # Calculate coach counts from historical attendance data if provided
    coach_counts_df = None
    if attendance_df is not None and "coach_name" in attendance_df.columns:
        logger.info("Calculating coach counts from historical attendance data...")
        coach_counts_df = calculate_coach_counts(attendance_df)
    
    # Get capacity for each session (base capacity × number of coaches)
    if coach_counts_df is not None:
        logger.info("Calculating capacity using coach counts...")
        df["capacity"] = df.apply(
            lambda row: get_capacity(
                row["session_name"],
                row["session_start"],
                system_config_df,
                coach_counts_df
            ),
            axis=1
        )
    else:
        logger.warning("No attendance data provided, using base capacity only (assumes 1 coach)")
        df["capacity"] = df.apply(
            lambda row: get_capacity(
                row["session_name"],
                row["session_start"],
                system_config_df,
                None,
                1  # Default to 1 coach
            ),
            axis=1
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
