"""
Configuration management for the forecasting pipeline.

Loads environment variables and validates required settings.
"""

import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class for the forecasting pipeline."""
    
    # Supabase credentials (required)
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_SERVICE_ROLE_KEY: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    
    # Model configuration
    RIDGE_ALPHA: float = 1.0  # Regularization strength for Ridge regression
    FORECAST_HORIZON_DAYS: int = 42  # Number of days to forecast ahead (6 weeks)
    
    # Training configuration
    TEST_SIZE_DAYS: int = 30  # Number of days to use for test set (from end of data)
    
    # Feature engineering
    ROLLING_WINDOW_4: int = 4  # Rolling average window size
    ROLLING_WINDOW_8: int = 8  # Rolling average window size
    
    @classmethod
    def validate(cls) -> None:
        """
        Validate that all required configuration is present.
        
        Raises:
            ValueError: If required configuration is missing.
        """
        missing = []
        
        if not cls.SUPABASE_URL:
            missing.append("SUPABASE_URL")
        if not cls.SUPABASE_SERVICE_ROLE_KEY:
            missing.append("SUPABASE_SERVICE_ROLE_KEY")
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"Please create a .env file with these values (see .env.example)."
            )


# Validate configuration on import
Config.validate()
