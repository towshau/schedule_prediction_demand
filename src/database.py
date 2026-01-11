"""
Database operations for Supabase.

Handles client initialization and data operations.
"""

import logging
from typing import List, Dict, Any
import pandas as pd
from supabase import create_client, Client
from src.config import Config

logger = logging.getLogger(__name__)


def get_supabase_client() -> Client:
    """
    Initialize and return a Supabase client.
    
    Returns:
        Client: Initialized Supabase client
        
    Raises:
        ValueError: If configuration is invalid
    """
    Config.validate()
    
    client = create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_ROLE_KEY)
    logger.info("Supabase client initialized successfully")
    return client


def execute_query(client: Client, query: str) -> List[Dict[str, Any]]:
    """
    Execute a raw SQL query and return results.
    
    Args:
        client: Supabase client
        query: SQL query string
        
    Returns:
        List of dictionaries containing query results
    """
    try:
        # Supabase Python client uses rpc for custom queries or table().select() for standard queries
        # For raw SQL, we'll need to use the REST API or create a database function
        # For MVP, we'll use table().select() with filters
        logger.warning(
            "Raw SQL execution not directly supported by Supabase client. "
            "Consider using table().select() or create database functions."
        )
        # Placeholder - actual implementation depends on query type
        return []
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def query_table_to_dataframe(client: Client, table_name: str, columns: str = "*") -> pd.DataFrame:
    """
    Query a Supabase table and return as pandas DataFrame.
    
    Args:
        client: Supabase client
        table_name: Name of the table to query
        columns: Column names to select (default: "*" for all)
        
    Returns:
        DataFrame containing table data
    """
    try:
        response = client.table(table_name).select(columns).execute()
        df = pd.DataFrame(response.data)
        logger.info(f"Retrieved {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error querying table {table_name}: {e}")
        raise


def upsert_forecasts(client: Client, df: pd.DataFrame) -> None:
    """
    Upsert forecast DataFrame to session_forecast_next_14_days table.
    
    The DataFrame must have columns:
    - session_date
    - session_name
    - session_start
    - session_end (optional)
    - predicted_attendance
    - predicted_utilisation (optional)
    - risk_flag
    
    Args:
        client: Supabase client
        df: DataFrame containing forecast data
    """
    try:
        # Convert date and time objects to strings for JSON serialization
        df_serializable = df.copy()
        
        # Convert date to string
        if "session_date" in df_serializable.columns:
            df_serializable["session_date"] = pd.to_datetime(df_serializable["session_date"]).dt.strftime("%Y-%m-%d")
        
        # Convert time to string (handle time objects and strings)
        if "session_start" in df_serializable.columns:
            # Convert time object to string if needed
            df_serializable["session_start"] = df_serializable["session_start"].apply(
                lambda x: str(x) if hasattr(x, 'strftime') else (x.strftime("%H:%M:%S") if hasattr(x, 'strftime') else str(x))
            )
        
        if "session_end" in df_serializable.columns:
            df_serializable["session_end"] = df_serializable["session_end"].apply(
                lambda x: str(x) if pd.notna(x) else None
            )
        
        # Convert numeric columns to float (handle NaN - keep as None for optional columns)
        numeric_cols = ["predicted_attendance", "predicted_utilisation"]
        for col in numeric_cols:
            if col in df_serializable.columns:
                # Convert to nullable float type
                df_serializable[col] = df_serializable[col].astype("Float64")
                # Replace NaN with None for JSON serialization
                df_serializable[col] = df_serializable[col].replace({pd.NA: None, float('nan'): None})
        
        # Remove duplicates based on unique constraint columns before upserting
        unique_cols = ["session_date", "session_name", "session_start"]
        if all(col in df_serializable.columns for col in unique_cols):
            # Keep last occurrence of duplicates (most recent forecast)
            df_serializable = df_serializable.drop_duplicates(
                subset=unique_cols,
                keep="last"
            )
            logger.info(f"Removed duplicates: {len(df)} rows -> {len(df_serializable)} unique rows")
        
        # Convert DataFrame to list of dictionaries
        records = df_serializable.to_dict("records")
        
        # Upsert data (idempotent due to unique constraint)
        response = client.table("session_forecast_next_14_days").upsert(
            records,
            on_conflict="session_date,session_name,session_start"
        ).execute()
        
        logger.info(f"Successfully upserted {len(records)} forecast records")
        
    except Exception as e:
        logger.error(f"Error upserting forecasts: {e}")
        raise
