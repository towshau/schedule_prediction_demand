"""
Model training and evaluation.

Implements time-aware train/test split and Ridge regression model.
"""

import logging
from typing import Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error

logger = logging.getLogger(__name__)


def time_aware_split(
    df: pd.DataFrame, 
    test_size_days: int = 30,
    date_col: str = "session_date"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split data by time, ensuring no future data leaks into training set.
    
    Training set: all data before cutoff date
    Test set: last N days of data
    
    Args:
        df: DataFrame with date column
        test_size_days: Number of days to use for test set (from end)
        date_col: Name of the date column
        
    Returns:
        Tuple of (train_df, test_df)
    """
    df = df.copy()
    
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not found in DataFrame")
    
    # Ensure date column is datetime
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Sort by date
    df = df.sort_values(date_col).reset_index(drop=True)
    
    # Find cutoff date (test_size_days from the end)
    max_date = df[date_col].max()
    cutoff_date = max_date - timedelta(days=test_size_days)
    
    # Split
    train_df = df[df[date_col] < cutoff_date].copy()
    test_df = df[df[date_col] >= cutoff_date].copy()
    
    # Convert dates back to original format
    train_df[date_col] = train_df[date_col].dt.date
    test_df[date_col] = test_df[date_col].dt.date
    
    logger.info(
        f"Time-aware split: {len(train_df)} training samples, {len(test_df)} test samples"
    )
    logger.info(f"Training period: {train_df[date_col].min()} to {train_df[date_col].max()}")
    logger.info(f"Test period: {test_df[date_col].min()} to {test_df[date_col].max()}")
    
    return train_df, test_df


def train_ridge_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    alpha: float = 1.0
) -> Ridge:
    """
    Train a Ridge regression model.
    
    Ridge regression adds L2 regularization (alpha parameter) to prevent overfitting.
    Higher alpha = more regularization (simpler model, less overfitting).
    
    Args:
        X_train: Training feature matrix
        y_train: Training target values
        alpha: Regularization strength (default: 1.0)
        
    Returns:
        Fitted Ridge regression model
    """
    logger.info(f"Training Ridge regression model with alpha={alpha}")
    
    model = Ridge(alpha=alpha, random_state=42)
    model.fit(X_train, y_train)
    
    logger.info("Model training completed")
    logger.info(f"Model coefficients: {len(model.coef_)} features")
    
    return model


def evaluate_model(
    model: Ridge,
    X_test: pd.DataFrame,
    y_test: pd.Series
) -> dict:
    """
    Evaluate model performance on test set.
    
    Args:
        model: Trained model
        X_test: Test feature matrix
        y_test: Test target values
        
    Returns:
        Dictionary with evaluation metrics
    """
    # Predictions
    y_pred = model.predict(X_test)
    
    # Ensure predictions are non-negative (attendance can't be negative)
    y_pred = np.maximum(y_pred, 0)
    
    # Calculate metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(np.mean((y_test - y_pred) ** 2))
    
    # Additional statistics
    mean_actual = y_test.mean()
    mean_pred = y_pred.mean()
    
    metrics = {
        "mae": mae,
        "rmse": rmse,
        "mean_actual_attendance": mean_actual,
        "mean_predicted_attendance": mean_pred,
        "mae_percentage": (mae / mean_actual * 100) if mean_actual > 0 else 0
    }
    
    logger.info("Model Evaluation Results:")
    logger.info(f"  Mean Absolute Error (MAE): {mae:.2f}")
    logger.info(f"  Root Mean Squared Error (RMSE): {rmse:.2f}")
    logger.info(f"  MAE as % of mean: {metrics['mae_percentage']:.1f}%")
    logger.info(f"  Mean actual attendance: {mean_actual:.2f}")
    logger.info(f"  Mean predicted attendance: {mean_pred:.2f}")
    
    return metrics
