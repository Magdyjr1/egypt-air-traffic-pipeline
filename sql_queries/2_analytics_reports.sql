/*
 * Egypt Air Traffic Pipeline - Analytical Queries
 * -----------------------------------------------
 * Description: Standard SQL queries used for reporting and visualization.
 * Key Metrics: Market Share, Top Routes, and Traffic Volume.
 */

-- ==========================================
-- Report 1: Top 10 Airlines (Market Share)
-- ==========================================
-- Calculates the percentage of flights per airline relative to total traffic.
SELECT 
    COALESCE(c.full_name, t.airline, 'Unknown') as airline_name,
    COUNT(*) as total_flights,
    ROUND(
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM egypt_sky_traffic WHERE airline IS NOT NULL), 
        2
    ) as percentage
FROM egypt_sky_traffic t
LEFT JOIN airline_codes c ON t.airline = c.code
WHERE t.airline IS NOT NULL
GROUP BY t.airline, c.full_name
ORDER BY total_flights DESC
LIMIT 10;

-- ==========================================
-- Report 2: Top 10 Flight Origins (Airports)
-- ==========================================
-- Identifies the most frequent airports sending flights into Egyptian airspace.
SELECT 
    COALESCE(a.airport_name, t.origin_airport, 'Unknown') as airport_name,
    COALESCE(a.country, '-') as country,
    COUNT(*) as total_flights,
    ROUND(
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM egypt_sky_traffic WHERE origin_airport != 'Unknown'), 
        2
    ) as percentage
FROM egypt_sky_traffic t
LEFT JOIN airport_codes a ON t.origin_airport = a.code
WHERE t.origin_airport != 'Unknown'
GROUP BY t.origin_airport, a.airport_name, a.country
ORDER BY total_flights DESC
LIMIT 10;