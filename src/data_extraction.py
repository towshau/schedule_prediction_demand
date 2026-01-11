"""
Data extraction from Supabase tables.

Pulls data from all required tables with column validation.
"""

import logging
from typing import List
import pandas as pd
from supabase import Client
from src.database import get_supabase_client, query_table_to_dataframe

logger = logging.getLogger(__name__)


# Required columns for each table
REQUIRED_COLUMNS = {
    "member_daily_sessions_attended": [
        "session_date", "session_start", "session_end", 
        "session_name", "coach_name", "member_id"
    ],
    "work_calendar": ["the_date", "is_business_day", "holiday_name"],
    "member_holds": ["member_id", "hold_start", "hold_end"],
    "system_config": ["config_key", "capacity", "match_pattern"]
}


def validate_columns(df: pd.DataFrame, table_name: str) -> None:
    """
    Validate that DataFrame contains all required columns.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table (for error messages)
        
    Raises:
        ValueError: If required columns are missing
    """
    if table_name not in REQUIRED_COLUMNS:
        logger.warning(f"No required columns defined for {table_name}, skipping validation")
        return
    
    required = REQUIRED_COLUMNS[table_name]
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        raise ValueError(
            f"Table {table_name} is missing required columns: {', '.join(missing)}. "
            f"Found columns: {', '.join(df.columns)}"
        )
    
    logger.info(f"Table {table_name} validation passed ({len(df)} rows)")


def extract_attendance_data(client: Client) -> pd.DataFrame:
    """
    Extract all data from member_daily_sessions_attended table.
    
    Args:
        client: Supabase client
        
    Returns:
        DataFrame with attendance data
    """
    logger.info("Extracting attendance data from member_daily_sessions_attended")
    df = query_table_to_dataframe(client, "member_daily_sessions_attended")
    validate_columns(df, "member_daily_sessions_attended")
    return df


def extract_work_calendar(client: Client) -> pd.DataFrame:
    """
    Extract work calendar data with business days and holidays.
    
    Args:
        client: Supabase client
        
    Returns:
        DataFrame with calendar data
    """
    logger.info("Extracting work calendar data")
    df = query_table_to_dataframe(client, "work_calendar")
    validate_columns(df, "work_calendar")
    
    # Ensure the_date is datetime
    if "the_date" in df.columns:
        df["the_date"] = pd.to_datetime(df["the_date"]).dt.date
    
    return df


def extract_member_holds(client: Client) -> pd.DataFrame:
    """
    Extract member hold periods.
    
    Args:
        client: Supabase client
        
    Returns:
        DataFrame with member hold data
    """
    logger.info("Extracting member holds data")
    df = query_table_to_dataframe(client, "member_holds")
    validate_columns(df, "member_holds")
    
    # Ensure dates are datetime
    for col in ["hold_start", "hold_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date
    
    return df


def extract_system_config(client: Client) -> pd.DataFrame:
    """
    Extract system configuration including session capacities.
    
    Args:
        client: Supabase client
        
    Returns:
        DataFrame with system configuration
    """
    logger.info("Extracting system configuration")
    df = query_table_to_dataframe(client, "system_config")
    validate_columns(df, "system_config")
    return df


def extract_all_data(client: Client = None) -> dict:
    """
    Extract all required data from Supabase.
    
    Args:
        client: Optional Supabase client (creates new one if not provided)
        
    Returns:
        Dictionary containing:
        - attendance: DataFrame from member_daily_sessions_attended
        - work_calendar: DataFrame from work_calendar
        - member_holds: DataFrame from member_holds
        - system_config: DataFrame from system_config
    """
    if client is None:
        client = get_supabase_client()
    
    data = {
        "attendance": extract_attendance_data(client),
        "work_calendar": extract_work_calendar(client),
        "member_holds": extract_member_holds(client),
        "system_config": extract_system_config(client)
    }
    
    logger.info("All data extraction completed successfully")
    return data
