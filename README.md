# Schedule Prediction & Management System

A comprehensive gym management and forecasting system that predicts session attendance, manages coach schedules, tracks member health metrics, and facilitates coaching notes.

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Database Schema](#database-schema)
- [Forecasting Pipeline](#forecasting-pipeline)
- [Setup & Installation](#setup--installation)
- [Usage](#usage)
- [Features](#features)

---

## Overview

This system provides:

1. **Session Attendance Forecasting** - ML-powered predictions for the next 6 weeks
2. **Coach Schedule Management** - Track preferences and hard blocks
3. **Member Health Tracking** - InBody metrics, weight, body fat, etc.
4. **Coaching Notes** - Structured note-taking for member progress
5. **Capacity Planning** - Risk flags and utilization monitoring

---

## System Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        A[Member Attendance]
        B[Work Calendar]
        C[Member Holds]
        D[System Config]
        E[Health Metrics]
        F[Coach Preferences]
    end
    
    subgraph "Processing Layer"
        G[Data Extraction]
        H[Aggregation]
        I[Feature Engineering]
        J[ML Model - Ridge Regression]
    end
    
    subgraph "Application Layer"
        K[Forecasting Engine]
        L[Schedule Optimizer]
        M[Health Dashboard]
        N[Notes System]
    end
    
    subgraph "Output"
        O[14-Day Forecasts]
        P[Coach Schedules]
        Q[Member Reports]
        R[Retool Dashboard]
    end
    
    A --> G
    B --> G
    C --> G
    D --> G
    E --> M
    F --> L
    
    G --> H
    H --> I
    I --> J
    J --> K
    
    K --> O
    L --> P
    M --> Q
    N --> Q
    
    O --> R
    P --> R
    Q --> R
    R
    
    style A fill:#e1f5ff
    style B fill:#e1f5ff
    style C fill:#e1f5ff
    style D fill:#e1f5ff
    style E fill:#e1f5ff
    style F fill:#e1f5ff
    style J fill:#f3e5f5
    style K fill:#fff9c4
    style L fill:#fff9c4
    style M fill:#fff9c4
    style N fill:#fff9c4
    style R fill:#e8f5e9
```

---

## Database Schema

### Core Tables Relationship

```mermaid
erDiagram
    member_database ||--o{ member_memberships : "has"
    member_database ||--o{ member_health_metrics : "tracks"
    member_database ||--o{ member_coach_notes : "receives"
    member_database ||--o{ member_holds : "has"
    member_database ||--o{ member_daily_sessions_attended : "attends"
    
    staff_database ||--o{ member_memberships : "coaches"
    staff_database ||--o{ member_coach_notes : "writes"
    staff_database ||--o{ schedule_preferences : "submits"
    
    member_memberships ||--o{ membership_types : "has_type"
    
    schedule_preferences }o--|| schedule_periods : "for_period"
    
    member_database {
        uuid id PK
        text first_name
        text last_name
        date dob
        text email
        numeric initial_weight
        numeric initial_BF_percentage
    }
    
    member_health_metrics {
        uuid id PK
        uuid member_id FK
        numeric weight
        numeric bf
        numeric bfm
        numeric ffm
        numeric smm
        numeric bone_mineral_content
        numeric visceral_fat_level
        integer age
        integer inbody_score
        timestamp date_created
    }
    
    member_coach_notes {
        uuid id PK
        uuid member_id FK
        uuid coach_id FK
        coach_notes_type note_type
        text note_content
        timestamp created_at
        timestamp updated_at
    }
    
    staff_database {
        uuid id PK
        text coach_name
        text role
        active_inactive staff_status
        numeric rm_ceiling
        text home_gym
    }
    
    schedule_preferences {
        uuid id PK
        uuid staff_id FK
        uuid period_id FK
        text block
        text preference_type
        timestamp submitted_at
    }
```

### Forecasting Pipeline Tables

```mermaid
erDiagram
    member_daily_sessions_attended ||--o{ session_forecast_next_14_days : "trains"
    work_calendar ||--|| session_forecast_next_14_days : "validates"
    system_config ||--|| session_forecast_next_14_days : "provides_capacity"
    member_holds ||--|| session_forecast_next_14_days : "adjusts"
    
    member_daily_sessions_attended {
        date session_date
        time session_start
        time session_end
        text session_name
        text coach_name
        uuid member_id
    }
    
    session_forecast_next_14_days {
        date session_date
        text session_name
        time session_start
        numeric predicted_attendance
        numeric predicted_utilisation
        text risk_flag
        timestamp created_at
    }
    
    work_calendar {
        date the_date
        boolean is_business_day
        text holiday_name
    }
    
    system_config {
        text config_key
        numeric capacity
        text match_pattern
    }
    
    member_holds {
        uuid member_id
        date hold_start
        date hold_end
    }
```

---

## Forecasting Pipeline

### High-Level Flow

```mermaid
flowchart TD
    Start([Daily Trigger<br/>2 AM AEST]) --> Extract[Step 1: Extract Data<br/>Pull attendance, calendar,<br/>holds, and capacity data]
    
    Extract --> Aggregate[Step 2: Aggregate<br/>Session-level attendance counts]
    
    Aggregate --> Features[Step 3: Feature Engineering<br/>Add temporal, holiday,<br/>and lag features]
    
    Features --> Split[Step 4: Data Split<br/>Time-aware train/test split]
    
    Split --> Train[Step 5: Train Model<br/>Ridge Regression ML model]
    
    Train --> Evaluate[Step 6: Evaluate<br/>Measure prediction accuracy]
    
    Evaluate --> Forecast[Step 7: Generate Forecasts<br/>42-day predictions]
    
    Forecast --> Adjust[Step 8: Apply Holds<br/>Adjust for member absences]
    
    Adjust --> Risk[Step 9: Calculate Risk<br/>Utilization & risk flags]
    
    Risk --> Save[Step 10: Save Results<br/>Upsert to Supabase]
    
    Save --> End([6-Week Forecasts Ready])
    
    End -.->|Next Day| Extract
    
    style Start fill:#e1f5ff
    style Extract fill:#fff4e1
    style Aggregate fill:#fff4e1
    style Features fill:#e8f5e9
    style Split fill:#e8f5e9
    style Train fill:#f3e5f5
    style Evaluate fill:#f3e5f5
    style Forecast fill:#fff9c4
    style Adjust fill:#fff9c4
    style Risk fill:#ffebee
    style Save fill:#e1f5ff
    style End fill:#e1f5ff
```

### Model Features

```mermaid
mindmap
  root((Forecast<br/>Model))
    Temporal
      Day of Week
      Week of Year
    External
      Holiday Flag
      Business Day
    Historical
      Lag-1 Attendance
      Rolling Avg 4-Week
      Rolling Avg 8-Week
    Capacity
      Session Capacity
      Coach Count
    Adjustments
      Member Holds
      Session Patterns
```

### Risk Flag Calculation

```mermaid
flowchart LR
    A[Predicted Attendance] --> B{Compare to Capacity}
    B --> C{< 80%}
    B --> D{80-95%}
    B --> E{> 95%}
    B --> F{No Capacity Data}
    
    C --> G[🟢 GREEN<br/>Low Risk]
    D --> H[🟡 AMBER<br/>Medium Risk]
    E --> I[🔴 RED<br/>High Risk]
    F --> J[⚫ BLACK<br/>Unknown]
    
    style G fill:#c8e6c9
    style H fill:#fff9c4
    style I fill:#ffcdd2
    style J fill:#e0e0e0
```

---

## Setup & Installation

### Prerequisites

- Python 3.10+
- Supabase account
- GitHub account (for automated runs)

### Local Installation

```bash
# Clone repository
git clone https://github.com/towshau/schedule_prediction_supply.git
cd schedule_prediction_supply

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials:
# SUPABASE_URL=your_project_url
# SUPABASE_SERVICE_ROLE_KEY=your_service_key
```

### Database Setup

Run the SQL scripts to create required tables:

```bash
# Core tables
cat sql/create_forecast_table.sql | supabase db execute
cat sql/create_member_cardio_time_trials.sql | supabase db execute

# Enums
cat sql/create_cardio_timetrial_enum.sql | supabase db execute
```

### GitHub Actions Setup

1. Go to repository Settings → Secrets
2. Add secrets:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
3. Workflow runs daily at 2 AM UTC automatically

---

## Usage

### Run Forecast Manually

```bash
python run_forecast.py
```

### Query Forecasts

```python
from src.database import get_supabase_client

client = get_supabase_client()

# Get next 14 days of forecasts
response = client.table('session_forecast_next_14_days')\
    .select('*')\
    .gte('session_date', 'today')\
    .order('session_date')\
    .execute()

forecasts = response.data
```

### Add Coach Notes

```python
# Via Retool or direct SQL
INSERT INTO member_coach_notes (member_id, coach_id, note_type, note_content)
VALUES (
    'member-uuid',
    'coach-uuid', 
    'goal',
    'Wants to hit 95kg squat by March'
);
```

---

## Features

### 1. Session Forecasting

- **42-day predictions** for all session types
- **ML-powered** using Ridge Regression
- **Risk flags** (Green/Amber/Red/Black)
- **Utilization tracking** (capacity %)
- **Member hold adjustments**

### 2. Coach Management

- **Schedule preferences** (HARD blocks, soft preferences)
- **Capacity planning** (RM ceiling tracking)
- **Multi-gym support** (BLIGH, BRIDGE, etc.)
- **Role-based filtering**

### 3. Member Health Tracking

- **InBody metrics** (weight, BF%, muscle mass, etc.)
- **Progress tracking** over time
- **Initial vs current** comparisons
- **Plotly visualizations** in Retool

### 4. Coaching Notes

- **Structured note types** (goal, habits, general, other)
- **Timestamped entries**
- **Coach attribution**
- **Full history tracking**

### 5. Analytics & Reporting

- **View by coach** - member counts, capacity
- **View by member** - health trends, notes
- **Calendar integration** - schedule visualization
- **Export capabilities**

---

## Model Details

### Algorithm: Ridge Regression

- **Type:** Linear regression with L2 regularization
- **Alpha:** 1.0 (prevents overfitting)
- **Training:** Time-aware split (last 30 days = test)
- **Features:** 13 total (temporal + lag + rolling averages)

### Performance Metrics

- **MAE** (Mean Absolute Error) - logged after each run
- **RMSE** (Root Mean Squared Error) - measure of variance
- **R²** (Coefficient of Determination) - model fit quality

### Continuous Improvement

The model retrains daily with:
- ✅ New attendance data
- ✅ Updated member patterns
- ✅ Recent hold information
- ✅ Seasonal adjustments

---

## Project Structure

```
Schedule/
├── README.md                       # This file
├── PIPELINE_FLOW.md               # Detailed pipeline docs
├── requirements.txt               # Python dependencies
├── .env                          # Environment variables (create this)
├── .env.example                  # Template
├── .gitignore
│
├── .github/
│   └── workflows/
│       └── daily_forecast.yml    # Automated daily runs
│
├── sql/
│   ├── create_forecast_table.sql
│   ├── create_member_cardio_time_trials.sql
│   └── create_cardio_timetrial_enum.sql
│
├── src/
│   ├── __init__.py
│   ├── config.py                  # Configuration management
│   ├── database.py                # Supabase operations
│   ├── data_extraction.py         # Data pull from Supabase
│   ├── aggregation.py             # Session-level aggregation
│   ├── feature_engineering.py     # ML feature creation
│   ├── model_training.py          # Ridge regression training
│   ├── forecasting.py             # Prediction generation
│   └── data_loading.py            # Risk flags & capacity
│
└── run_forecast.py                # Main pipeline orchestrator
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Missing environment variables | Check `.env` file exists with correct values |
| Database connection fails | Verify Supabase URL and service key |
| Model accuracy poor | Ensure 2-3 months of historical data available |
| Forecast dates missing | Check `work_calendar` has future dates |
| GitHub Actions fail | Verify secrets are set in repository settings |

### Logging

All operations are logged:
```bash
# View logs
tail -f logs/forecast.log

# Or check GitHub Actions logs in the Actions tab
```

---

## Future Enhancements

- [ ] Coach-specific prediction models
- [ ] Seasonal pattern detection
- [ ] Real-time attendance tracking
- [ ] Automated waitlist management
- [ ] Member journey stage forecasting
- [ ] Advanced scheduling optimization
- [ ] Mobile app integration

---

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/YourFeature`)
3. Commit changes (`git commit -m 'Add YourFeature'`)
4. Push to branch (`git push origin feature/YourFeature`)
5. Open Pull Request

---

## License

[MIT License](LICENSE)

---

## Support

For issues or questions:
- 📧 Email: [your-email]
- 💬 Slack: [your-slack-channel]
- 📝 Issues: [GitHub Issues](https://github.com/towshau/schedule_prediction_supply/issues)

---

**Built with ❤️ for better gym management**
