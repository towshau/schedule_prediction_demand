# Schedule Prediction Demand

Prediction engine to map demand of existing members and their behaviour patterns.

This MVP forecasting pipeline predicts session attendance for the next 14 days using historical member attendance data, machine learning (Ridge regression), and various business rules.

## Overview

The pipeline:
1. Pulls historical attendance data from Supabase
2. Aggregates member-level data to session occurrence level
3. Builds features (temporal, holiday flags, lag/rolling averages)
4. Trains a Ridge regression model with time-aware train/test split
5. Generates 14-day forecasts for all session slots
6. Applies member holds adjustments
7. Calculates capacity utilization and risk flags
8. Writes results back to Supabase

## Architecture

```
Supabase Tables:
├── member_daily_sessions_attended (data source)
├── work_calendar (business days & holidays)
├── member_holds (member hold periods)
├── system_config (session capacities)
└── session_forecast_next_14_days (output table)
```

## Setup

### Prerequisites

- Python 3.10 or higher
- Supabase account with required tables
- Access to Supabase Service Role Key

### Installation

1. **Clone the repository** (if you haven't already):
   ```bash
   git clone https://github.com/towshau/schedule_prediction_demand.git
   cd schedule_prediction_demand
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
   
   Create a `.env` file in the project root:
   ```bash
   # Copy from .env.example or create manually
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
   ```
   
   Replace the placeholders with your actual Supabase credentials.

5. **Create the forecast table in Supabase**:
   
   Run the SQL script to create the output table:
   ```bash
   # Execute the SQL in your Supabase SQL editor
   cat sql/create_forecast_table.sql
   ```
   
   Or manually execute the contents of `sql/create_forecast_table.sql` in your Supabase dashboard.

## Usage

### Running Locally

Run the forecasting pipeline manually:

```bash
python run_forecast.py
```

The script will:
- Extract data from Supabase
- Train the model
- Generate forecasts
- Write results back to `session_forecast_next_14_days` table

### Automated Daily Runs (GitHub Actions)

The pipeline is configured to run automatically via GitHub Actions.

**Setup GitHub Secrets:**

1. Go to your repository settings
2. Navigate to Secrets and variables → Actions
3. Add the following secrets:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase service role key

**Workflow Schedule:**

The workflow runs daily at 2:00 AM UTC (see `.github/workflows/daily_forecast.yml`). 

To adjust the schedule, edit the cron expression:
```yaml
- cron: '0 2 * * *'  # Hour Minute Day Month DayOfWeek
```

**Manual Trigger:**

You can also trigger the workflow manually:
1. Go to Actions tab in GitHub
2. Select "Daily Forecast Pipeline"
3. Click "Run workflow"

### Running with Cron (Alternative)

If you prefer to run locally on a schedule:

```bash
# Edit crontab
crontab -e

# Add this line (runs daily at 2 AM)
0 2 * * * cd /path/to/schedule_prediction_demand && /path/to/venv/bin/python run_forecast.py >> logs/forecast.log 2>&1
```

## Database Schema

### Input Tables

**member_daily_sessions_attended:**
- `session_date` (date)
- `session_start` (time)
- `session_end` (time)
- `session_name` (text)
- `coach_name` (text)
- `member_id` (uuid)
- One row per member attendance per session

**work_calendar:**
- `the_date` (date)
- `is_business_day` (boolean)
- `holiday_name` (text, nullable)

**member_holds:**
- `member_id` (uuid)
- `hold_start` (date)
- `hold_end` (date, nullable)

**system_config:**
- `config_key` (text)
- `capacity` (numeric)
- `match_pattern` (text) - Pattern to match session names (e.g., "PERFORM", "BOX", "VO2")

### Output Table

**session_forecast_next_14_days:**
- `session_date` (date)
- `session_name` (text)
- `session_start` (time)
- `session_end` (time, nullable)
- `predicted_attendance` (numeric)
- `predicted_utilisation` (numeric, nullable)
- `risk_flag` (text) - Values: 'green', 'amber', 'red', 'black'
- `created_at` (timestamp)
- Unique constraint on `(session_date, session_name, session_start)`

## Model Details

### Features

The model uses the following features:

1. **Temporal Features:**
   - `day_of_week`: Day of week (0=Monday, 6=Sunday)
   - `week_of_year`: Week number in year

2. **Holiday Feature:**
   - `is_holiday`: Boolean flag (true if non-business day or holiday)

3. **Lag Features:**
   - `lag_1_attendance`: Previous occurrence's attendance for the same session slot
   - `rolling_avg_4`: Rolling average of last 4 occurrences
   - `rolling_avg_8`: Rolling average of last 8 occurrences

**Data Leakage Prevention:** All lag/rolling features use only prior data (via `.shift()` and proper windowing). The training/test split is time-aware (no random shuffling).

### Model

- **Algorithm:** Ridge Regression (L2 regularization)
- **Alpha:** 1.0 (regularization strength)
- **Train/Test Split:** Last 30 days used for testing (time-aware, no random split)

### Risk Flags

Risk flags are calculated based on predicted utilization:

- **green:** utilisation < 0.80 (low risk)
- **amber:** utilisation 0.80 - 0.95 (medium risk)
- **red:** utilisation > 0.95 (high risk)
- **black:** capacity data missing (unknown risk)

### Member Holds Adjustment

For MVP, the pipeline uses a simple aggregate approach:
- Counts all members on hold per forecast date
- Subtracts this count from predicted attendance for ALL sessions on that date

This over-adjusts (assumes held members would attend all sessions) but is acceptable for MVP. Future enhancement: Track which sessions each member typically attends.

## Project Structure

```
Schedule/
├── README.md
├── requirements.txt
├── .env                    # Create this with your Supabase credentials
├── .gitignore
├── .github/
│   └── workflows/
│       └── daily_forecast.yml
├── sql/
│   └── create_forecast_table.sql
├── src/
│   ├── __init__.py
│   ├── config.py           # Configuration management
│   ├── database.py         # Supabase client and DB operations
│   ├── data_extraction.py  # Data extraction from Supabase
│   ├── aggregation.py      # Session-level aggregation
│   ├── feature_engineering.py  # Feature creation
│   ├── model_training.py   # Model training and evaluation
│   ├── forecasting.py      # Forecast generation
│   └── data_loading.py     # Capacity matching and risk flags
└── run_forecast.py         # Main orchestration script
```

## Troubleshooting

### Missing Environment Variables

If you see errors about missing environment variables:
- Ensure `.env` file exists in project root
- Verify `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are set correctly

### Missing Database Columns

If you see validation errors about missing columns:
- Verify all required tables exist in Supabase
- Check that table schemas match the expected columns (see Database Schema section)

### Model Performance

If model predictions seem inaccurate:
- Check that you have sufficient historical data (at least 2-3 months recommended)
- Verify lag features are being calculated correctly (check logs)
- Consider adjusting `RIDGE_ALPHA` in `src/config.py` for stronger/weaker regularization

### GitHub Actions Failures

If the workflow fails:
- Check Actions tab for error logs
- Verify GitHub Secrets are set correctly
- Ensure the workflow file path is correct (`.github/workflows/daily_forecast.yml`)

## Logging

The pipeline logs important information at each step:
- Row counts at each stage
- Model training metrics (MAE, RMSE)
- Risk flag distribution
- Forecast summary statistics

Logs are written to stdout and can be captured in GitHub Actions logs or redirected to a file.

## Future Enhancements

- Member-level session preference tracking (for more accurate hold adjustments)
- Additional features (e.g., coach-specific patterns, seasonal trends)
- Model retraining schedule optimization
- Forecast confidence intervals
- A/B testing framework for model improvements

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
