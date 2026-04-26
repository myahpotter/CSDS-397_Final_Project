import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text
import os
from dotenv import load_dotenv

url = "https://raw.githubusercontent.com/myahpotter/CSDS-397_Final_Project/refs/heads/main/streaming.csv"
df = pd.read_csv(url)

load_dotenv()

username = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
host = os.environ.get('DB_HOST')
dbname = os.environ.get('DB_NAME')

engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:5432/{dbname}")
df.to_sql('streams_source', engine, schema="source", if_exists='replace', index=False)

with engine.connect() as conn:
    conn.execute(text("ALTER TABLE source.streams_source ADD COLUMN is_new BOOLEAN"))
    conn.execute(text("UPDATE source.streams_source SET is_new = FALSE"))
    conn.commit()
