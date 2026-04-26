DROP TABLE IF EXISTS streams_staging.songs, streams_staging.artists, streams_staging.features,
streams_staging.entries CASCADE;


-- Creating Songs, Parent_Labels, Labels, Artists, and Features Tables 
-- Songs has the debut rank, debut peak, debut date, and max weeks in charts 
CREATE TABLE IF NOT EXISTS streams_staging."songs"
(
	SongID SERIAL PRIMARY KEY,
	SongTitle VARCHAR(200),
	SongRank INT,
	PeakPosition INT, 
	WeeksInCharts INT,
	DebutDate DATE
);

CREATE TABLE IF NOT EXISTS streams_staging."artists"
(
	ArtistID SERIAL PRIMARY KEY,
	ArtistName VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS streams_staging."features" 
(
	SongID INT REFERENCES streams_staging."songs"(SongID),
	ArtistID INT REFERENCES streams_staging."artists"(ArtistID), 
	Contribution VARCHAR (10) CHECK (Contribution IN ('Primary', 'Feature')),
	PRIMARY KEY (SongID, ArtistID)
);

-- Entries has the current entry date, songid, current rank, current peak position, last week rank, and current weeks in charts 
CREATE TABLE IF NOT EXISTS streams_staging."entries"
(
	EntryDate DATE, 
	SongID INT REFERENCES streams_staging."songs"(SongID),
	SongRank INT,
	PeakPosition INT,
	LastWeek INT,
	WeeksInCharts INT,
	PRIMARY KEY (EntryDate, SongID)
);




