-- Populating Songs Table
WITH song_debuts AS 
(
	SELECT DISTINCT "Song", MIN("Date") as DebutDate
	FROM source."streams_source" 
	GROUP BY "Song"
),
	maximum_time AS 
(
	SELECT "Song", MAX("Weeks in Charts") as max_weeks
	FROM source."streams_source" 
	GROUP BY "Song"
)
INSERT INTO streams_staging."songs" (SongTitle, SongRank, PeakPosition, WeeksInCharts, DebutDate) 
SELECT song_debuts."Song", streams_source."Rank",
streams_source."Peak Position", maximum_time.max_weeks,  song_debuts.DebutDate
FROM song_debuts INNER JOIN source."streams_source" ON song_debuts."Song" = streams_source."Song" 
AND song_debuts.DebutDate = streams_source."Date"
LEFT JOIN maximum_time ON song_debuts."Song" = maximum_time."Song";


-- Populating Entries Table
INSERT INTO streams_staging."entries" (EntryDate, SongID, SongRank, PeakPosition, LastWeek, WeeksInCharts)
SELECT streams_source."Date", songs.SongID, streams_source."Rank", streams_source."Peak Position", streams_source."Last Week",
streams_source."Weeks in Charts"
FROM source."streams_source" INNER JOIN streams_staging."songs" ON streams_source."Song" = songs.songtitle
ON CONFLICT (EntryDate, SongID) DO NOTHING;


-- Populating Artists Table
WITH artists AS 
(
	SELECT UNNEST(STRING_TO_ARRAY("Artist", ',')) as artist_name
	FROM source."streams_source" 
	WHERE "Artist" NOT LIKE '%Tyler, The Creator%'
	UNION
	SELECT UNNEST(STRING_TO_ARRAY(Featured_Artist, ',')) as artist_name
	FROM source."streams_source"
	WHERE (Featured_Artist IS NOT NULL) AND Featured_Artist NOT LIKE '%Tyler, The Creator%'
)
INSERT INTO streams_staging."artists" (ArtistName)
SELECT DISTINCT TRIM(artist_name)
FROM artists
ON CONFLICT (ArtistName) DO NOTHING;


-- Populating Features Table with Primary Artists
INSERT INTO streams_staging."features" (SongID, ArtistID, Contribution)
SELECT DISTINCT SongID, ArtistID, 'Primary'
FROM source."streams_source" INNER JOIN streams_staging."songs"
ON streams_source."Song" = songs.SongTitle
CROSS JOIN LATERAL (SELECT TRIM(UNNEST(STRING_TO_ARRAY("Artist", ','))) AS artist
WHERE "Artist" NOT LIKE '%Tyler, The Creator%') expanded
INNER JOIN streams_staging."artists"
ON expanded.artist = artists.ArtistName
ON CONFLICT (SongID, ArtistID) DO NOTHING;


-- Populating Features Table with Featured Artists 
INSERT INTO streams_staging."features" (SongID, ArtistID, Contribution)
SELECT DISTINCT SongID, ArtistID, 'Feature'
FROM source."streams_source" INNER JOIN streams_staging."songs"
ON streams_source."Song" = songs.SongTitle
CROSS JOIN LATERAL (SELECT TRIM(UNNEST(STRING_TO_ARRAY(Featured_Artist, ','))) AS artist 
WHERE Featured_Artist NOT LIKE '%Tyler, The Creator%') expanded
INNER JOIN streams_staging."artists"
ON expanded.artist = artists.ArtistName
WHERE streams_source.Featured_Artist IS NOT NULL
ON CONFLICT (SongID, ArtistID) DO NOTHING;
