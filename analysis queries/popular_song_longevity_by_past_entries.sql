DROP TABLE IF EXISTS billboard_analysis."popular_song_longevity_by_artist";

CREATE TABLE billboard_analysis."popular_song_longevity_by_artist" AS
WITH artist_hits AS (
	SELECT DISTINCT artists.artistname, songs.songid, songs.weeksincharts
	FROM streams_staging."songs" INNER JOIN streams_staging."features"
	ON songs.songid = features.songid INNER JOIN streams_staging.artists 
	ON features.artistid = artists.artistid
	WHERE songs.peakposition <= 5 AND features.contribution = 'Primary'
)
SELECT artistname, COUNT(DISTINCT songid) AS hit_songs,
ROUND(AVG(weeksincharts)::numeric, 2) AS avg_weeks_in_chart
FROM artist_hits
GROUP BY artist_hits.artistname
ORDER BY hit_songs DESC;

SELECT * FROM billboard_analysis."popular_song_longevity_by_artist";
