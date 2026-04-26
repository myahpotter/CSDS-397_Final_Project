ALTER TABLE source."streams_source" ADD COLUMN Featured_Artist VARCHAR(100);
-- ALTER TABLE streams_staging."songs" ADD COLUMN DebutDate DATE;

-- Typecasting Date Column
ALTER TABLE source."streams_source" ALTER COLUMN "Date" TYPE DATE USING ("Date"::date);
UPDATE source."streams_source" 
SET 
	"Last Week" = 0
WHERE "Last Week" ILIKE '%-%';
ALTER TABLE source."streams_source" ALTER COLUMN "Last Week" TYPE INT USING ("Last Week"::int);

-- Trimming Quotes from Artist Names
UPDATE source."streams_source"
SET 
	"Artist" = TRIM(BOTH '"' FROM "Artist")
WHERE "Artist" ILIKE '%"%';


-- Separating Artists and Featured Artists
UPDATE source."streams_source"
SET 
	Featured_Artist = TRIM(SPLIT_PART("Artist", 'Featuring', 2)),
	"Artist" = TRIM(SPLIT_PART("Artist", 'Featuring', 1))
WHERE "Artist" ILIKE '%Featuring%';

UPDATE source."streams_source"
SET 
	Featured_Artist = TRIM(SPLIT_PART("Artist", 'Feat.', 2)),
	"Artist" = TRIM(SPLIT_PART("Artist", 'Feat.', 1))
WHERE "Artist" ILIKE '%Feat.%';


-- Standardizing the Artist Separations
UPDATE source."streams_source"
SET 
	"Artist" = REGEXP_REPLACE("Artist", '\s+(x|&|\+|/|with|duet with)\s+', ' , ', 'gi')
WHERE (
	"Artist" ~* '\s+(x|&|\+|/|with|duet with)\s+'
)
	AND "Artist" NOT LIKE ALL (
	ARRAY['%Mumford & Sons%', '%Simon & Garfunkel%', '%She & Him%', '%Nico & Vinz%', 
	'%Zay Hilfigerrr & Zayion McCall%', '%Dan + Shay%', '%Florence + The Machine%',
	'%Lil Nas X%', '%X Ambassadors%', '%Audrey Nuna & REI AMI%']
);

-- Manual Artist Format Corrections
UPDATE source."streams_source"
SET 
	"Artist" = REPLACE("Artist", '&', ',')
WHERE "Artist" ILIKE '%Lil Nas X%' OR "Artist" ILIKE '%Dan + Shay%' 
OR "Artist" ILIKE '%Florence + The Machine%';


UPDATE source."streams_source"
SET 
	"Artist" = TRIM(SPLIT_PART("Artist", '<a href="https://www.billboard.com/artist/lil-Nas-x/">', 2))
WHERE "Artist" ILIKE '%<a href="https://www.billboard.com/artist/lil-Nas-x/">%';
 

-- Standardizing the Featured Artist Separations
UPDATE source."streams_source"
SET 
	Featured_Artist = REGEXP_REPLACE(Featured_Artist, '\s+(x|&|\+|/|with|duet with)\s+', ' , ', 'gi')
WHERE (
   	Featured_Artist ~* '\s+(x|&|\+|/|with|duet with)\s+'
)
	AND Featured_Artist NOT LIKE ALL (
	ARRAY['%Mumford & Sons%', '%Simon & Garfunkel%', '%She & Him%', '%Nico & Vinz%', 
	'%Zay Hilfigerrr & Zayion McCall%', '%Dan + Shay%', '%Florence + The Machine%',
	'%Lil Nas X%', '%X Ambassadors%', '%Audrey Nuna & REI AMI%']
);


-- 




-- Data Cleaning 
