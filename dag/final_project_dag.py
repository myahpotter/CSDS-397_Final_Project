from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLTableCheckOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import billboard
import re
import kaggle
import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text
from psycopg2.extras import execute_batch

# -------------------------
# Define DAG with description
# -------------------------
billboard_analysis_dag = DAG(
    dag_id="billboard_data_dag",
    description="Billboard DAG to ingest new chart data and analyze the findings.",
    start_date=datetime(2026,4,1),
    schedule="0 22 * * 3",
    catchup=False,
    default_args={
        'retries':3,
        'retry_delay': timedelta(seconds=5),
        'max_retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': False,
        'email': ['mxp736@case.edu'], 
        'email_on_failure': True,             
    }
)

def ingest_billboard_data():

    username = os.environ.get('DB_USERNAME')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    dbname = os.environ.get('DB_NAME')

    kaggle.api.authenticate()

    kaggle.api.dataset_download_file(
        'ludmin/billboard',
        file_name='streaming.csv',
        path='/tmp'
    )

    engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:5432/{dbname}")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(date) FROM source.streams_source"))
        last_loaded_date = result.scalar()
    df = pd.read_csv('/tmp/streaming.csv')
    df.replace('-', 0, inplace=True)
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    new_rows = df[df['Date'] > last_loaded_date]
    new_rows_updated = new_rows.assign(is_new = 'True')
    new_rows_updated.to_sql('streams_source', engine, schema="source", if_exists='append', index=False)

# -------------------------
# Task 1: Ingest new Billboard entries for the most recent week 
# -------------------------
weekly_ingestion = PythonOperator(
    task_id='weekly_ingestion',
    python_callable = ingest_billboard_data,
    dag=billboard_analysis_dag
)

# Have to update ranks each week I
# -------------------------
# Task 2: Clean new Billboard entries
# -------------------------
cleaning_new_entries = SQLExecuteQueryOperator(
    task_id='cleaning_new_entries',
    sql=r"""
    -- Trimming Quotes from Artist Names
    UPDATE source."streams_source"
    SET 
	    "Artist" = TRIM(BOTH '"' FROM "Artist")
    WHERE "Artist" ILIKE '%"%' AND is_new = TRUE;


    -- Separating Artists and Featured Artists
    UPDATE source."streams_source"
    SET 
	    Featured_Artist = TRIM(SPLIT_PART("Artist", 'Featuring', 2)),
	    "Artist" = TRIM(SPLIT_PART("Artist", 'Featuring', 1))
    WHERE "Artist" ILIKE '%Featuring%' AND is_new = TRUE;

    UPDATE source."streams_source"
    SET 
	    Featured_Artist = TRIM(SPLIT_PART("Artist", 'Feat.', 2)),
	    "Artist" = TRIM(SPLIT_PART("Artist", 'Feat.', 1))
    WHERE "Artist" ILIKE '%Feat.%' AND is_new = TRUE;


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
    )
        AND is_new = TRUE;

    -- Manual Artist Format Corrections
    UPDATE source."streams_source"
    SET 
	    "Artist" = REPLACE("Artist", '&', ',')
    WHERE (
        "Artist" ILIKE '%Lil Nas X%' OR "Artist" ILIKE '%Dan + Shay%' 
        OR "Artist" ILIKE '%Florence + The Machine%'
    ) 
        AND is_new = TRUE;

    UPDATE source."streams_source"
    SET 
	    "Artist" = TRIM(SPLIT_PART("Artist", '<a href="https://www.billboard.com/artist/lil-Nas-x/">', 2))
    WHERE "Artist" ILIKE '%<a href="https://www.billboard.com/artist/lil-Nas-x/">%' AND is_new = TRUE;
    """,
    conn_id='my_postgres',
    dag=billboard_analysis_dag,
)

# -------------------------
# Task 3: Populate tables with new Billboard entries
# -------------------------
populate_tables = SQLExecuteQueryOperator(
    task_id='populate_tables',
    sql="""
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
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=billboard_analysis_dag,
)

# -------------------------
# Task 4: Set all new Billboard entries to old
# -------------------------
update_new_entries = SQLExecuteQueryOperator(
    task_id='update_new_entries',
    sql="""
    UPDATE source."streams_source"
    SET 
	    is_new = FALSE
    WHERE is_new = TRUE;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=billboard_analysis_dag,
)

# -------------------------
# Task 5: Create peak_positions_by_month analysis table
# -------------------------
peak_positions_by_month = SQLExecuteQueryOperator(
    task_id='create_staging_schema',
    sql="""
    DROP TABLE IF EXISTS billboard_analysis."positions_and_longevity_by_debut_month";

    CREATE TABLE billboard_analysis."positions_and_longevity_by_debut_month" AS
    SELECT EXTRACT(MONTH FROM songs.DebutDate) as month_number,
    TO_CHAR(TO_TIMESTAMP(EXTRACT(MONTH FROM songs.DebutDate)::text, 'MM'), 'Month') AS debut_month,
    ROUND(AVG(PeakPosition)::numeric, 2) AS average_peak_position,
    ROUND(AVG(WeeksInCharts)::numeric, 2) AS average_longevity
    FROM streams_staging.songs 
    GROUP BY month_number, debut_month 
    ORDER BY month_number ASC;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=billboard_analysis_dag,
)

# -------------------------
# Task 6: Create popular_song_longevity_by_artist analysis table 
# -------------------------
popular_song_longevity_by_artist = SQLExecuteQueryOperator(
    task_id='popular_song_longevity_by_artist',
    sql="""
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
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=billboard_analysis_dag,
)

# -------------------------
# Task 7: Create positions_and_longevity_by_peak_month table
# -------------------------
positions_and_longevity_by_peak_month = SQLExecuteQueryOperator(
    task_id='positions_and_longevity_by_peak_month',
    sql="""
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
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=billboard_analysis_dag,
)

# -------------------------
# Task 8: Create performance_by_salary_analysis
# -------------------------
#performance_by_salary_analysis = SQLExecuteQueryOperator(
    #task_id='performance_by_salary_analysis',
    #sql="""
    #DROP TABLE IF EXISTS gold."performance_by_salary_analysis";

    #CREATE TABLE gold."performance_by_salary_analysis" AS
    #SELECT performancerating AS performance_rating, ROUND(AVG(salary)::numeric, 2) AS average_salary
    #FROM staging."employee"
    #GROUP BY performancerating
    #ORDER BY performancerating ASC;
   # """,
    #conn_id='my_postgres',
   # autocommit=True,
    #dag=billboard_analysis_dag,
#)

# -------------------------
# Define task dependencies
# -------------------------
(weekly_ingestion >> cleaning_new_entries >> populate_tables >> update_new_entries >> 
[peak_positions_by_month, popular_song_longevity_by_artist, positions_and_longevity_by_peak_month])