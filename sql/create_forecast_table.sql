-- Create the session forecast table for storing 14-day predictions
-- This table stores daily forecasts for each session slot

CREATE TABLE IF NOT EXISTS public.session_forecast_next_14_days (
    session_date DATE NOT NULL,
    session_name TEXT NOT NULL,
    session_start TIME NOT NULL,
    session_end TIME,
    predicted_attendance NUMERIC NOT NULL,
    predicted_utilisation NUMERIC,
    risk_flag TEXT NOT NULL CHECK (risk_flag IN ('green', 'amber', 'red', 'black')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint for idempotent upserts
    CONSTRAINT session_forecast_unique UNIQUE (session_date, session_name, session_start)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_session_forecast_date ON public.session_forecast_next_14_days(session_date);
CREATE INDEX IF NOT EXISTS idx_session_forecast_session ON public.session_forecast_next_14_days(session_name, session_start);

-- Add comment to table
COMMENT ON TABLE public.session_forecast_next_14_days IS 'Stores 14-day ahead forecasts for session attendance with capacity utilization and risk flags';
