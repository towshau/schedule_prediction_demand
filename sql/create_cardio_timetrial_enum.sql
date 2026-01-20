-- Create enum type for cardio time trial tests
-- This enum defines all valid cardio time trial test types

CREATE TYPE cardio_timetrial_tests AS ENUM (
    '5 Minute Bike Erg Test',
    '5 Minute Row Erg Test',
    '12 Minute Run Test',
    '5 km Bike Erg Time Trial',
    '5 km Row Erg Time Trial',
    '1.6 km Run Time Trial',
    '5 km Run Time Trial',
    '30 Second Bike Erg Test',
    '30 Second Assault Bike Test'
);

-- Add comment to enum type
COMMENT ON TYPE cardio_timetrial_tests IS 'Enum defining all valid cardio time trial test types for member assessments';
