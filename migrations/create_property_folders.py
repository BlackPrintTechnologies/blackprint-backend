-- Migration script to create property folders tables
-- Run this script in your PostgreSQL database

-- Create property_folders table
CREATE TABLE IF NOT EXISTS property_folders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    is_default BOOLEAN DEFAULT FALSE,
    status INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_property_folders_user
        FOREIGN KEY (user_id) REFERENCES bp_users(bp_user_id)
        ON DELETE CASCADE
);

-- Create folder_properties table
CREATE TABLE IF NOT EXISTS folder_properties (
    id SERIAL PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    fid INTEGER NOT NULL,
    config_city VARCHAR(50) DEFAULT 'mexico',
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    CONSTRAINT fk_folder_properties_folder
        FOREIGN KEY (folder_id) REFERENCES property_folders(id)
        ON DELETE CASCADE
);

-- Create unique constraints
ALTER TABLE property_folders 
ADD CONSTRAINT unique_user_folder_name 
UNIQUE (user_id, name, status);

ALTER TABLE folder_properties 
ADD CONSTRAINT unique_folder_property 
UNIQUE (folder_id, fid, config_city);

-- Create indexes for better performance
CREATE INDEX idx_property_folders_user_id ON property_folders(user_id);
CREATE INDEX idx_property_folders_status ON property_folders(status);
CREATE INDEX idx_folder_properties_folder_id ON folder_properties(folder_id);
CREATE INDEX idx_folder_properties_fid ON folder_properties(fid);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_property_folders_updated_at 
    BEFORE UPDATE ON property_folders 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Insert default "Liked" folder for existing users (optional)
-- This will create a default folder for each existing user
INSERT INTO property_folders (user_id, name, description, is_default, created_at, updated_at)
SELECT DISTINCT bp_user_id, 'Liked', 'Default folder for liked properties', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM bp_users
WHERE bp_user_id NOT IN (
    SELECT DISTINCT user_id FROM property_folders WHERE is_default = TRUE
);
