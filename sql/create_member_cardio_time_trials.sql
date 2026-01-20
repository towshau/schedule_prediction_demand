-- Create the member cardio time trials table
-- This table stores cardio time trial test results for members

CREATE TABLE IF NOT EXISTS public.member_cardio_time_trials (
    id SERIAL PRIMARY KEY,
    member_id UUID NOT NULL,
    coach_id INTEGER NOT NULL,
    test_type cardio_timetrial_tests NOT NULL,
    day_created DATE NOT NULL DEFAULT CURRENT_DATE,
    distance_m NUMERIC,
    duration_minutes INTEGER,
    duration_seconds NUMERIC,
    session_notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    CONSTRAINT fk_member FOREIGN KEY (member_id) REFERENCES public.member_database(id) ON DELETE CASCADE
    -- CONSTRAINT fk_coach FOREIGN KEY (coach_id) REFERENCES public.staff(id) ON DELETE RESTRICT
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_member_cardio_member_id ON public.member_cardio_time_trials(member_id);
CREATE INDEX IF NOT EXISTS idx_member_cardio_coach_id ON public.member_cardio_time_trials(coach_id);
CREATE INDEX IF NOT EXISTS idx_member_cardio_day_created ON public.member_cardio_time_trials(day_created);
CREATE INDEX IF NOT EXISTS idx_member_cardio_test_type ON public.member_cardio_time_trials(test_type);

-- Add comments
COMMENT ON TABLE public.member_cardio_time_trials IS 'Stores cardio time trial test results for members with coach tracking';
COMMENT ON COLUMN public.member_cardio_time_trials.distance_m IS 'Distance covered in meters';
COMMENT ON COLUMN public.member_cardio_time_trials.duration_minutes IS 'Duration minutes component (e.g., 25 for 25:30)';
COMMENT ON COLUMN public.member_cardio_time_trials.duration_seconds IS 'Duration seconds component (e.g., 30 for 25:30)';
