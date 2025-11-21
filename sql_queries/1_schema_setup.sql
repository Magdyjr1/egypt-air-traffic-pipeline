/*
 * Egypt Air Traffic Pipeline - Schema Setup
 * ------------------------------------------
 * Description: This script initializes the database schema.
 * It creates the main Fact table for flight traffic and Dimension tables
 * for looking up Airline and Airport names.
 */

-- ==========================================
-- 1. Fact Table: Flight Traffic Data
-- ==========================================

-- Note: Uncomment the DROP line only if you want to reset data (WARNING: Data Loss)
-- DROP TABLE IF EXISTS egypt_sky_traffic;

CREATE TABLE IF NOT EXISTS egypt_sky_traffic (
    flight_id SERIAL PRIMARY KEY,        -- Unique Sequence ID
    icao24 VARCHAR(50),                  -- Transponder Hex Code
    callsign VARCHAR(20),                -- Flight Number (e.g., MS985)
    airline VARCHAR(50),                 -- IATA Airline Code
    origin_airport VARCHAR(10),          -- IATA Airport Code
    latitude FLOAT,                      -- GPS Latitude
    longitude FLOAT,                     -- GPS Longitude
    altitude_meters FLOAT,               -- Altitude in Meters
    velocity_kmh FLOAT,                  -- Speed in Km/h
    heading FLOAT,                       -- Flight Direction (Degrees)
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Data Collection Timestamp
);

-- Ensure the timestamp column exists (for legacy data compatibility)
ALTER TABLE egypt_sky_traffic 
ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- ==========================================
-- 2. Dimension Table: Airlines Dictionary
-- ==========================================
CREATE TABLE IF NOT EXISTS airline_codes (
    code VARCHAR(5) PRIMARY KEY,
    full_name VARCHAR(100)
);

-- Populate Airline Codes (Enrichment Data)
INSERT INTO airline_codes (code, full_name) VALUES 
('MS', 'EgyptAir'),
('SV', 'Saudia'),
('QR', 'Qatar Airways'),
('EK', 'Emirates'),
('TK', 'Turkish Airlines'),
('FZ', 'Flydubai'),
('XY', 'Flynas'),
('J9', 'Jazeera Airways'),
('EY', 'Etihad Airways'),
('LH', 'Lufthansa'),
('BA', 'British Airways'),
('AF', 'Air France'),
('SM', 'Air Cairo'),
('ET', 'Ethiopian Airlines'),
('RJ', 'Royal Jordanian'),
('PC', 'Pegasus Airlines'),
('NE', 'Nesma Airlines'),
('WY', 'Oman Air'),
('GF', 'Gulf Air'),
('ME', 'Middle East Airlines'),
('W6', 'Wizz Air')
ON CONFLICT (code) DO NOTHING;

-- ==========================================
-- 3. Dimension Table: Airports Dictionary
-- ==========================================
CREATE TABLE IF NOT EXISTS airport_codes (
    code VARCHAR(10) PRIMARY KEY,
    airport_name VARCHAR(255),
    country VARCHAR(100)
);

-- Populate Airport Codes (Enrichment Data)
INSERT INTO airport_codes (code, airport_name, country) VALUES 
('CAI', 'Cairo International Airport', 'Egypt'),
('HRG', 'Hurghada International Airport', 'Egypt'),
('SSH', 'Sharm El Sheikh International Airport', 'Egypt'),
('HBE', 'Borg El Arab Airport', 'Egypt'),
('LXR', 'Luxor International Airport', 'Egypt'),
('SPX', 'Sphinx International Airport', 'Egypt'),
('JED', 'King Abdulaziz Int. Airport', 'Saudi Arabia'),
('RUH', 'King Khalid Int. Airport', 'Saudi Arabia'),
('DXB', 'Dubai International Airport', 'UAE'),
('DOH', 'Hamad International Airport', 'Qatar'),
('AMM', 'Queen Alia International Airport', 'Jordan'),
('KWI', 'Kuwait International Airport', 'Kuwait'),
('IST', 'Istanbul Airport', 'Turkey'),
('LHR', 'London Heathrow Airport', 'UK'),
('CDG', 'Charles de Gaulle Airport', 'France'),
('FCO', 'Rome Fiumicino Airport', 'Italy'),
('TLV', 'Ben Gurion Airport', '-')
ON CONFLICT (code) DO NOTHING;