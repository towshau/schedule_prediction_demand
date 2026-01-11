# Forecasting Pipeline Flow Diagram

This diagram shows how the forecasting pipeline works from start to finish.

```mermaid
flowchart TD
    Start([Daily Trigger<br/>2 AM AEST]) --> Extract[Step 1: Extract Data<br/>Get historical attendance records,<br/>calendar, member holds, and<br/>capacity settings from database]
    
    Extract --> Aggregate[Step 2: Aggregate Data<br/>Count how many members<br/>attended each session occurrence<br/>Change from individual records<br/>to session-level totals]
    
    Aggregate --> Features[Step 3: Build Features<br/>Add helpful information like<br/>day of week, holidays,<br/>previous attendance patterns<br/>These help the model make predictions]
    
    Features --> Split[Step 4: Split Data<br/>Separate into training data<br/>older dates and test data<br/>recent dates to validate accuracy]
    
    Split --> Train[Step 5: Train Model<br/>Teach the model to predict<br/>attendance using historical patterns<br/>Uses machine learning to find trends]
    
    Train --> Evaluate[Step 6: Evaluate Model<br/>Test the model on recent data<br/>to see how accurate it is<br/>Measure prediction errors]
    
    Evaluate --> Forecast[Step 7: Generate Forecasts<br/>Create predictions for next 6 weeks<br/>For each session slot and date<br/>Predict how many will attend]
    
    Forecast --> Adjust[Step 8: Apply Member Holds<br/>Adjust predictions for members<br/>who are on hold<br/>Only subtract from sessions<br/>they normally attend]
    
    Adjust --> Calculate[Step 9: Calculate Risk<br/>Compare predictions to capacity<br/>Calculate utilization percentage<br/>Flag sessions as green, amber, or red<br/>based on risk level]
    
    Calculate --> Save[Step 10: Save to Database<br/>Write all forecasts to Supabase<br/>Update existing records or<br/>create new ones]
    
    Save --> End([Complete<br/>6-week forecasts ready])
    
    %% Learning Loop - Model improves over time
    End -.->|Next Day<br/>New Data Added| Extract
    Save -.->|Actual Results<br/>Become Training Data| Extract
    
    style Start fill:#e1f5ff
    style Extract fill:#fff4e1
    style Aggregate fill:#fff4e1
    style Features fill:#e8f5e9
    style Split fill:#e8f5e9
    style Train fill:#f3e5f5
    style Evaluate fill:#f3e5f5
    style Forecast fill:#fff9c4
    style Adjust fill:#fff9c4
    style Calculate fill:#ffebee
    style Save fill:#e1f5ff
    style End fill:#e1f5ff
```

## Step-by-Step Explanation

### Step 1: Extract Data
**What it does:** Pulls all the information we need from the database
- Gets individual member attendance records (who attended which sessions)
- Gets the work calendar (which days are business days, holidays)
- Gets member hold information (who is on hold and when)
- Gets capacity settings (how many people each session type can hold)

**Why it's important:** This is our starting point - we need all this data to make predictions

---

### Step 2: Aggregate Data
**What it does:** Counts attendance at the session level instead of member level
- Changes from "Member X attended Session Y" records
- To "Session Y had 15 attendees on this date"
- One row per unique session occurrence (date + session name + time)

**Why it's important:** The model needs to predict session-level attendance, not individual member attendance

---

### Step 3: Build Features
**What it does:** Adds helpful information that helps the model make better predictions
- **Day of week:** Monday, Tuesday, etc. (attendance patterns vary by day)
- **Week of year:** Helps identify seasonal patterns
- **Holiday flags:** Sessions often have different attendance on holidays
- **Previous attendance (lag):** How many attended the last time this session ran
- **Rolling averages:** Average attendance over the last 4 and 8 occurrences

**Why it's important:** These features help the model learn patterns like "Monday mornings are busier" or "this session usually has 10 people"

---

### Step 4: Split Data
**What it does:** Separates data into training and testing sets
- **Training set:** Older data used to teach the model
- **Test set:** Recent data (last 30 days) used to check if the model learned correctly
- Split by time (not random) to prevent using future information

**Why it's important:** We need to test the model on data it hasn't seen to verify it will work for future forecasts

---

### Step 5: Train Model
**What it does:** Teaches the model how to predict attendance
- Looks at all the training data
- Finds patterns and relationships between features and attendance
- Learns things like "if it's Monday and the last session had 12 people, predict 13"
- Uses Ridge Regression (a type of machine learning)

**Why it's important:** This is where the model "learns" how attendance works based on historical patterns

---

### Step 6: Evaluate Model
**What it does:** Tests how good the model's predictions are
- Uses the test set (recent data the model hasn't seen)
- Makes predictions for those dates
- Compares predictions to actual attendance
- Calculates error metrics (how far off the predictions were)

**Why it's important:** This tells us if the model is accurate enough to use. We log the error so you can see how well it's performing

---

### Step 7: Generate Forecasts
**What it does:** Creates predictions for the next 6 weeks
- For each unique session slot (session name + start time)
- For each forecast date (next 42 business days)
- Uses the trained model to predict attendance
- Creates thousands of forecast records

**Why it's important:** This is the main output - predictions for all sessions over the next 6 weeks

---

### Step 8: Apply Member Holds
**What it does:** Adjusts predictions for members who are on hold
- Identifies which members are on hold and during which dates
- Looks at each member's historical attendance patterns (which sessions they usually attend)
- Subtracts their expected attendance from those specific sessions only
- Only adjusts for dates within their hold period (after hold ends, they're included normally)
- Ignores holds without end dates completely

**Why it's important:** This makes predictions more accurate by accounting for members who won't be attending because they're on hold

---

### Step 9: Calculate Risk
**What it does:** Determines how risky each session is based on predicted attendance
- Matches each session to its capacity setting (how many people it can hold)
- Calculates utilization (predicted attendance ÷ capacity)
- Assigns risk flags:
  - **GREEN:** Utilization < 80% (plenty of space, low risk)
  - **AMBER:** Utilization 80-95% (getting full, medium risk)
  - **RED:** Utilization > 95% (over capacity, high risk)
  - **BLACK:** Capacity data missing (unknown risk)

**Why it's important:** This helps you identify which sessions might be overbooked so you can plan ahead

---

### Step 10: Save to Database
**What it does:** Writes all the forecasts to the Supabase database
- Creates or updates records in the `session_forecast_next_14_days` table
- One record per session slot per forecast date
- If a forecast already exists (same date + session + time), it updates it
- If it doesn't exist, it creates a new record

**Why it's important:** This makes the forecasts available for your application to use, display, or query

---

## Data Sources

The pipeline pulls data from these Supabase tables:
- `member_daily_sessions_attended` - Individual member attendance records
- `work_calendar` - Business days and holiday information
- `member_holds` - Member hold periods (start and end dates)
- `system_config` - Session capacity settings

## Output

The pipeline writes forecasts to:
- `session_forecast_next_14_days` - Contains forecasts with:
  - Session date, name, and start time
  - Predicted attendance
  - Predicted utilization (percentage of capacity)
  - Risk flag (green/amber/red/black)

---

## Key Concepts

### Why Split by Time (Not Random)
We split data by date to make sure the model only learns from past data. If we randomly mixed dates, the model might accidentally learn from the future, which would make it seem more accurate than it actually is.

### Why Member Holds Adjustment Matters
Without this adjustment, if 50 members are on hold, we'd subtract 50 from every session, even sessions those members never attend. The smart adjustment only subtracts from sessions each member typically attends, making predictions much more accurate.

### Why Risk Flags Help
Risk flags give you a quick visual way to see which sessions need attention:
- **RED sessions** might need extra capacity or waitlist management
- **GREEN sessions** have plenty of space and could potentially accept more bookings
- **AMBER sessions** are in the watch zone - monitor closely

---

## How Predictions Are Calculated

### The Mathematical Model

The pipeline uses **Ridge Regression** to predict attendance. This is a type of linear model that finds the best relationship between features (input information) and attendance (what we want to predict).

### The Prediction Formula

The model calculates predictions using this mathematical equation:

**Predicted Attendance = (w₁ × feature₁) + (w₂ × feature₂) + (w₃ × feature₃) + ... + (wₙ × featureₙ) + bias**

Where:
- **w₁, w₂, w₃, ... wₙ** are weights (coefficients) that the model learns during training
- **feature₁, feature₂, feature₃, ... featureₙ** are the input features for each session
- **bias** is a base value the model adjusts

### Features Used in Predictions

The model uses these features to make predictions:

1. **Day of week** (7 features: Monday=1 if Monday, 0 otherwise, etc.)
2. **Week of year** (1 feature: number from 1-52)
3. **Holiday flag** (1 feature: 1 if holiday, 0 otherwise)
4. **Lag-1 attendance** (1 feature: attendance at the previous occurrence of this session)
5. **Rolling average (4 weeks)** (1 feature: average attendance over last 4 occurrences)
6. **Rolling average (8 weeks)** (1 feature: average attendance over last 8 occurrences)

### How the Model Learns

During training, the model:
1. Looks at all historical session data with actual attendance
2. Finds the best weights (w₁, w₂, etc.) that minimize prediction error
3. Uses Ridge regularization to prevent overfitting (keeps weights small)
4. Produces a formula it can use to predict future attendance

### Example Calculation

For a Monday morning PERFORM session on week 15 (non-holiday) with:
- Previous attendance: 8 people
- 4-week average: 9 people
- 8-week average: 8.5 people

The model might calculate:
- `(0.5 × Monday) + (0.1 × Week15) + (0 × Holiday) + (0.7 × Lag8) + (0.3 × Avg4) + (0.2 × Avg8) + 2.0`
- `= (0.5 × 1) + (0.1 × 15) + (0 × 0) + (0.7 × 8) + (0.3 × 9) + (0.2 × 8.5) + 2.0`
- `= 0.5 + 1.5 + 0 + 5.6 + 2.7 + 1.7 + 2.0 = 14.0 people`

*(Note: Actual weights are learned from your data and will be different)*

### Model Improvement Over Time

Each day when the pipeline runs:
- New actual attendance data is added to the training dataset
- The model re-trains using all available historical data
- Updated weights are calculated that better match recent patterns
- New forecasts use these improved weights, making predictions more accurate over time
