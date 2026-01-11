"""
Aggregation of member-level data to session occurrence level.

Transforms individual member attendance records into session-level aggregates.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def aggregate_to_session_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate member-level attendance data to session occurrence level.
    
    One row per unique session occurrence (session_date + session_name + session_start + session_end).
    Aggregates actual_attendance as count of distinct member_id.
    
    Args:
        df: DataFrame with member-level attendance data
            Required columns: session_date, session_name, session_start, 
                             session_end, member_id
    
    Returns:
        DataFrame with one row per session occurrence:
        - session_date
        - session_name
        - session_start
        - session_end
        - actual_attendance (count of distinct member_id)
    """
    logger.info(f"Aggregating {len(df)} member-level records to session level")
    
    # Ensure required columns exist
    required_cols = ["session_date", "session_name", "session_start", "member_id"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for aggregation: {', '.join(missing)}")
    
    # Ensure session_date is datetime for proper sorting
    df = df.copy()
    if "session_date" in df.columns:
        df["session_date"] = pd.to_datetime(df["session_date"]).dt.date
    
    # Group by session occurrence and count distinct members
    # Note: session_end is optional but included if present
    groupby_cols = ["session_date", "session_name", "session_start"]
    if "session_end" in df.columns:
        groupby_cols.append("session_end")
    
    aggregated = (
        df.groupby(groupby_cols, as_index=False)
        .agg({
            "member_id": "nunique"  # Count distinct member_id
        })
        .rename(columns={"member_id": "actual_attendance"})
    )
    
    logger.info(f"Aggregated to {len(aggregated)} session occurrences")
    logger.info(f"Total attendance across all sessions: {aggregated['actual_attendance'].sum()}")
    
    return aggregated
