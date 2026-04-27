-- Songs that enter the charts in may through june reach higher peak positions than other songs
DROP TABLE IF EXISTS billboard_analysis."positions_and_longevity_by_debut_month";

CREATE TABLE billboard_analysis."positions_and_longevity_by_debut_month" AS
SELECT EXTRACT(MONTH FROM songs.DebutDate) as month_number,
TO_CHAR(TO_TIMESTAMP(EXTRACT(MONTH FROM songs.DebutDate)::text, 'MM'), 'Month') AS debut_month,
ROUND(AVG(PeakPosition)::numeric, 2) AS average_peak_position,
ROUND(AVG(WeeksInCharts)::numeric, 2) AS average_longevity
FROM streams_staging.songs 
GROUP BY month_number, debut_month 
ORDER BY month_number ASC;
	
SELECT * FROM billboard_analysis."positions_and_longevity_by_debut_month";

