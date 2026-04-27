-- Songs that enter the charts in may through june reach higher peak positions than other songs
DROP TABLE IF EXISTS billboard_analysis."positions_and_longevity_by_peak_month";

CREATE TABLE billboard_analysis."positions_and_longevity_by_peak_month" AS
SELECT EXTRACT(MONTH FROM entries.EntryDate) as month_number,
TO_CHAR(TO_TIMESTAMP(EXTRACT(MONTH FROM entries.EntryDate)::text, 'MM'), 'Month') AS peak_month,
ROUND(AVG(songs.PeakPosition)::numeric, 2) AS average_peak_position,
ROUND(AVG(songs.WeeksInCharts)::numeric, 2) AS average_longevity
FROM streams_staging.songs LEFT JOIN streams_staging.entries ON songs.songid = entries.songid 
AND songs.peakposition = entries.songrank
WHERE entrydate IS NOT NULL
GROUP BY month_number, peak_month 
ORDER BY month_number ASC;
	
SELECT * FROM billboard_analysis."positions_and_longevity_by_peak_month";