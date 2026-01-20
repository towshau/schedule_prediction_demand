-- Create Biomap_Supplements table
CREATE TABLE IF NOT EXISTS Biomap_Supplements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplement_name TEXT NOT NULL,
    benefit TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_biomap_supplements_name ON Biomap_Supplements(supplement_name);

-- Create trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_biomap_supplements_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_biomap_supplements_updated_at
    BEFORE UPDATE ON Biomap_Supplements
    FOR EACH ROW
    EXECUTE FUNCTION update_biomap_supplements_updated_at();
