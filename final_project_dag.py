from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLTableCheckOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import billboard
import re
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

    hook = PostgresHook(postgres_conn_id='my_postgres')
    engine = hook.get_sqlalchemy_engine()

    engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:5432/{dbname}")

    kaggle.api.authenticate()

    kaggle.api.dataset_download_file(
        'ludmin/billboard',
        file_name='streaming.csv',
        path='/tmp'
    )

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
    sql="""
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
# Task 5: Create staging schema tables
# -------------------------
create_staging_schema = SQLExecuteQueryOperator(
    task_id='create_staging_schema',
    sql="""
    DROP TABLE IF EXISTS staging."department", staging."employee", staging."support ratings", staging."total sales" CASCADE;
    CREATE TABLE IF NOT EXISTS staging."department" 
    (
	DepartmentID SERIAL PRIMARY KEY,
	DepartmentName VARCHAR(100)	UNIQUE
    );

    CREATE TABLE IF NOT EXISTS staging."employee"
    (
	EmployeeID INT PRIMARY KEY,
	EmployeeName VARCHAR(100),
	Age INT,
	Department VARCHAR(100) REFERENCES staging."department"(DepartmentName),
	DateOfJoining DATE,
	YearsOfExperience INT,
	Country VARCHAR(100),
	Salary FLOAT,
	PerformanceRating INT
    );

    CREATE TABLE IF NOT EXISTS staging."support ratings" 
    (
	EmployeeID INT REFERENCES staging."employee"(EmployeeID) PRIMARY KEY,
	SupportRating INT
    );

    CREATE TABLE IF NOT EXISTS staging."total sales" 
    (
	EmployeeID INT REFERENCES staging."employee"(EmployeeID) PRIMARY KEY,
	TotalSales FLOAT
    );
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 6: Normalize data by transferring into staging schema tables
# -------------------------
populating_staging_schema = SQLExecuteQueryOperator(
    task_id='populating_staging_schema',
    sql="""
    INSERT INTO staging."department"(DepartmentName) SELECT DISTINCT Department FROM sources."employee data";

    INSERT INTO staging."employee" SELECT EmployeeID, EmployeeName, Age, Department, DateOfJoining,
    YearsOfExperience, Country, Salary, PerformanceRating FROM sources."employee data";

    INSERT INTO staging."support ratings" SELECT EmployeeID, SupportRating FROM sources."employee data"
    WHERE department = 'Support';

    INSERT INTO staging."total sales" SELECT EmployeeID, TotalSales FROM sources."employee data" 
    WHERE department = 'Sales';
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 7: Create performance_by_department_analysis
# -------------------------
performance_by_department_analysis = SQLExecuteQueryOperator(
    task_id='performance_by_department_analysis',
    sql="""
    DROP TABLE IF EXISTS gold."performance_by_department_analysis";

    CREATE TABLE gold."performance_by_department_analysis" AS
    SELECT department.departmentname AS department_name, ROUND(AVG(employee.performancerating)::numeric, 2) AS average_performance
    FROM staging."employee" AS employee
    LEFT JOIN staging."department" AS department
    ON employee.department = department.departmentname
    GROUP BY department.departmentname;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 8: Create performance_by_salary_analysis
# -------------------------
performance_by_salary_analysis = SQLExecuteQueryOperator(
    task_id='performance_by_salary_analysis',
    sql="""
    DROP TABLE IF EXISTS gold."performance_by_salary_analysis";

    CREATE TABLE gold."performance_by_salary_analysis" AS
    SELECT performancerating AS performance_rating, ROUND(AVG(salary)::numeric, 2) AS average_salary
    FROM staging."employee"
    GROUP BY performancerating
    ORDER BY performancerating ASC;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 9: Create performance_by_tenure_analysis
# -------------------------
performance_by_tenure_analysis = SQLExecuteQueryOperator(
    task_id='performance_by_tenure_analysis',
    sql="""
    DROP TABLE IF EXISTS gold."performance_by_tenure_analysis";

    CREATE TABLE gold."performance_by_tenure_analysis" AS
    WITH tenure_separated AS (
    SELECT performancerating, 
    CASE 
    WHEN yearsofexperience >= 1 AND yearsofexperience < 6 THEN '0-5' 
    WHEN yearsofexperience >= 6 AND yearsofexperience < 11 THEN '6-10'
    WHEN yearsofexperience >= 11 AND yearsofexperience < 16 THEN '11-15' 
    WHEN yearsofexperience >= 16 AND yearsofexperience <= 20 THEN '16-20'
    END AS tenure
    FROM staging."employee"
    )
    SELECT tenure, ROUND(AVG(performancerating)::numeric, 2) AS average_performance
    FROM tenure_separated
    GROUP BY tenure
    ORDER BY split_part(tenure, '-', 1)::int;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 10: Create salary_to_department_analysis
# -------------------------
salary_to_department_analysis = SQLExecuteQueryOperator(
    task_id='salary_to_department_analysis',
    sql="""
    DROP TABLE IF EXISTS gold."salary_to_department_analysis";

    CREATE TABLE gold."salary_to_department_analysis" AS
    SELECT department AS department_name, ROUND(AVG(salary)::numeric, 2) AS average_salary
    FROM staging."employee"
    GROUP BY department;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 11: Create salary_to_tenure_analysis
# -------------------------
salary_to_tenure_analysis = SQLExecuteQueryOperator(
    task_id='salary_to_tenure_analysis',
    sql="""
    DROP TABLE IF EXISTS gold."salary_to_tenure_analysis";

    CREATE TABLE gold."salary_to_tenure_analysis" AS
    WITH tenure_separated AS (
    SELECT salary, 
    CASE 
    WHEN yearsofexperience >= 1 AND yearsofexperience < 6 THEN '0-5' 
    WHEN yearsofexperience >= 6 AND yearsofexperience < 11 THEN '6-10'
    WHEN yearsofexperience >= 11 AND yearsofexperience < 16 THEN '11-15' 
    WHEN yearsofexperience >= 16 AND yearsofexperience <= 20 THEN '16-20'
    END AS tenure
    FROM staging."employee"
    )
    SELECT tenure, ROUND(AVG(salary)::numeric, 2) AS average_salary
    FROM tenure_separated
    GROUP BY tenure
    ORDER BY split_part(tenure, '-', 1)::int;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Task 12: Create sales_by_performance_analysis
# -------------------------
sales_by_performance_analysis = SQLExecuteQueryOperator(
    task_id='sales_by_performance_analysis',
    sql="""
    DROP TABLE IF EXISTS gold."sales_by_performance_analysis";

    CREATE TABLE gold."sales_by_performance_analysis" AS
    SELECT employee.performancerating, ROUND(AVG(totalsales)::numeric, 2)
    FROM staging."employee" AS employee
    LEFT JOIN staging."total sales" AS total_sales
    ON employee.employeeid = total_sales.employeeid
    WHERE employee.department = 'Sales'
    GROUP BY employee.performancerating
    ORDER BY employee.performancerating ASC;
    """,
    conn_id='my_postgres',
    autocommit=True,
    dag=employee_analysis_dag,
)

# -------------------------
# Define task dependencies
# -------------------------
(weekly_ingestion >> cleaning_new_entries >> populate_tables >> update_new_entries >> create_staging_schema >> populating_staging_schema >>
[performance_by_department_analysis, performance_by_salary_analysis, performance_by_tenure_analysis, salary_to_department_analysis, salary_to_tenure_analysis,
sales_by_performance_analysis])