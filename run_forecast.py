#!/usr/bin/env python3
"""
Main orchestration script for the forecasting pipeline.

This script:
1. Pulls latest historical data from Supabase
2. Aggregates to session occurrence level
3. Builds features with proper leakage prevention
4. Trains Ridge regression model with time-aware split
5. Generates 6-week (42-day) forecasts
6. Applies member holds adjustment
7. Calculates capacity and risk flags
8. Upserts results to Supabase
"""

import logging
import sys
from datetime import date, timedelta
from src.config import Config
from src.database import get_supabase_client, upsert_forecasts
from src.data_extraction import extract_all_data
from src.aggregation import aggregate_to_session_level
from src.feature_engineering import (
    add_temporal_features,
    add_holiday_feature,
    add_lag_features,
    build_feature_matrix
)
from src.model_training import time_aware_split, train_ridge_model, evaluate_model
from src.forecasting import (
    get_session_slots,
    generate_forecast_dates,
    build_forecast_features,
    apply_member_holds_adjustment
)
from src.data_loading import prepare_forecast_output

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    try:
        logger.info("=" * 60)
        logger.info("Starting Forecasting Pipeline")
        logger.info("=" * 60)
        
        # Initialize Supabase client
        client = get_supabase_client()
        
        # Step 1: Extract all data
        logger.info("\n[Step 1] Extracting data from Supabase...")
        data = extract_all_data(client)
        
        attendance_df = data["attendance"]
        work_calendar_df = data["work_calendar"]
        member_holds_df = data["member_holds"]
        system_config_df = data["system_config"]
        
        logger.info(f"  Attendance records: {len(attendance_df)}")
        logger.info(f"  Calendar entries: {len(work_calendar_df)}")
        logger.info(f"  Member holds: {len(member_holds_df)}")
        logger.info(f"  System config entries: {len(system_config_df)}")
        
        # Step 2: Aggregate to session level
        logger.info("\n[Step 2] Aggregating to session occurrence level...")
        session_df = aggregate_to_session_level(attendance_df)
        logger.info(f"  Session occurrences: {len(session_df)}")
        
        # Step 3: Feature engineering
        logger.info("\n[Step 3] Building features...")
        session_df = add_temporal_features(session_df)
        session_df = add_holiday_feature(session_df, work_calendar_df)
        session_df = add_lag_features(session_df)
        
        # Step 4: Build feature matrix and split data
        logger.info("\n[Step 4] Preparing training data...")
        X, y = build_feature_matrix(session_df)
        
        # Time-aware split (no random shuffle!)
        train_df, test_df = time_aware_split(
            session_df,
            test_size_days=Config.TEST_SIZE_DAYS
        )
        
        X_train, y_train = build_feature_matrix(train_df)
        X_test, y_test = build_feature_matrix(test_df)
        
        logger.info(f"  Training samples: {len(X_train)}")
        logger.info(f"  Test samples: {len(X_test)}")
        
        # Step 5: Train model
        logger.info("\n[Step 5] Training Ridge regression model...")
        model = train_ridge_model(X_train, y_train, alpha=Config.RIDGE_ALPHA)
        
        # Evaluate on test set
        logger.info("\n[Step 6] Evaluating model...")
        metrics = evaluate_model(model, X_test, y_test)
        
        # Step 7: Generate forecasts
        logger.info("\n[Step 7] Generating forecasts for next 6 weeks (42 days)...")
        
        # Get unique session slots
        session_slots = get_session_slots(session_df)
        
        # Generate forecast dates (starting from tomorrow, excluding non-business days)
        today = date.today()
        forecast_start = today + timedelta(days=1)
        forecast_dates = generate_forecast_dates(
            forecast_start,
            horizon_days=Config.FORECAST_HORIZON_DAYS,
            work_calendar=work_calendar_df
        )
        
        logger.info(f"  Forecast dates: {forecast_dates[0]} to {forecast_dates[-1]}")
        logger.info(f"  Number of forecast dates: {len(forecast_dates)}")
        
        # Build forecast features
        forecast_df = build_forecast_features(
            session_slots,
            forecast_dates,
            session_df,  # Historical data with features
            work_calendar_df
        )
        
        # Build feature matrix for forecasts (exclude target and metadata)
        X_forecast, _ = build_feature_matrix(forecast_df)
        
        # Generate predictions
        predictions = model.predict(X_forecast)
        predictions = predictions.clip(min=0)  # Ensure non-negative
        
        forecast_df["predicted_attendance"] = predictions
        
        logger.info(f"  Generated {len(forecast_df)} forecast records")
        logger.info(f"  Average predicted attendance: {predictions.mean():.2f}")
        
        # Step 8: Apply member holds adjustment
        logger.info("\n[Step 8] Applying member holds adjustment...")
        forecast_df = apply_member_holds_adjustment(
            forecast_df,
            member_holds_df,
            forecast_dates,
            attendance_df  # Historical attendance data for calculating member patterns
        )
        
        # Step 9: Calculate capacity and risk flags
        logger.info("\n[Step 9] Calculating capacity utilisation and risk flags...")
        forecast_output = prepare_forecast_output(forecast_df, system_config_df)
        
        # Step 10: Upsert to Supabase
        logger.info("\n[Step 10] Writing forecasts to Supabase...")
        upsert_forecasts(client, forecast_output)
        
        logger.info("\n" + "=" * 60)
        logger.info("Forecasting Pipeline Completed Successfully!")
        logger.info("=" * 60)
        logger.info(f"\nSummary:")
        logger.info(f"  Forecast records written: {len(forecast_output)}")
        logger.info(f"  Model MAE: {metrics['mae']:.2f}")
        logger.info(f"  Forecast period: {forecast_dates[0]} to {forecast_dates[-1]}")
        
    except Exception as e:
        logger.error(f"\nPipeline failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
