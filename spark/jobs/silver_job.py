"""
silver_job.py — Job Spark Bronze → Silver
Aceita --source-date YYYY-MM-DD para processar só a partição do dia.
"""
import os, sys, argparse, requests, psycopg2
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-date", type=str, default=str(date.today()))
    return parser.parse_args()

TMDB_API_KEY   = os.getenv("TMDB_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minio123")
PG_HOST        = os.getenv("PG_HOST",     "postgres")
PG_PORT        = int(os.getenv("PG_PORT", "5432"))
PG_DB          = os.getenv("PG_DB",       "moviepulse")
PG_USER        = os.getenv("PG_USER",     "moviepulse")
PG_PASSWORD    = os.getenv("PG_PASSWORD", "moviepulse")
JDBC_URL       = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS     = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

def pg_execute(sql):
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()

def write_staging(df, table):
    count = df.count()
    df.write.jdbc(url=JDBC_URL, table=table, mode="overwrite", properties=JDBC_PROPS)
    print(f"[silver_job] staging -> {table} ({count} registros)")

def create_spark():
    return (SparkSession.builder.appName("MoviePulse-Bronze-to-Silver")
        .config("spark.hadoop.fs.s3a.impl",                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",              MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",              MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.endpoint",                MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access",       "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",  "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())

def read_bronze(spark, source_date):
    path = f"s3a://bronze/tmdb/trending_daily/dt={source_date}/"
    print(f"[silver_job] Lendo Bronze: {path}")
    df = spark.read.option("multiLine", "false").json(path)
    count = df.count()
    if count == 0:
        print(f"[silver_job] Nenhum registro em {path}. Encerrando.")
        sys.exit(0)
    print(f"[silver_job] {count} registros lidos (dt={source_date})")
    return df

def clean(df):
    return (df
        .withColumn("movie_id",          F.col("movie_id").cast("int"))
        .withColumn("title",             F.trim(F.col("title")))
        .withColumn("popularity",        F.col("popularity").cast("double"))
        .withColumn("vote_average",      F.col("vote_average").cast("double"))
        .withColumn("vote_count",        F.col("vote_count").cast("int"))
        .withColumn("adult",             F.col("adult").cast("boolean"))
        .withColumn("release_date",      F.to_date(F.col("release_date"),  "yyyy-MM-dd"))
        .withColumn("source_date",       F.to_date(F.col("source_date"),   "yyyy-MM-dd"))
        .withColumn("extraction_ts",     F.to_timestamp(F.col("extraction_ts")))
        .withColumn("original_language", F.trim(F.col("original_language")))
        .withColumn("original_title",    F.trim(F.col("raw.original_title")))
        .withColumn("overview",          F.trim(F.col("raw.overview")))
        .withColumn("poster_path",       F.col("raw.poster_path"))
        .withColumn("genre_ids",         F.col("raw.genre_ids"))
        .withColumn("processed_at",      F.current_timestamp())
        .filter(F.col("movie_id").isNotNull())
        .filter(F.col("title").isNotNull()))

def build_dim_genres(spark):
    resp = requests.get("https://api.themoviedb.org/3/genre/movie/list",
        params={"api_key": TMDB_API_KEY, "language": "pt-BR"}, timeout=15)
    resp.raise_for_status()
    genres = resp.json()["genres"]
    return spark.createDataFrame([(g["id"], g["name"]) for g in genres], schema=["genre_id", "genre_name"])

def build_dim_movies(df):
    w = Window.partitionBy("movie_id").orderBy(F.col("extraction_ts").desc())
    return (df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
        .select("movie_id","title","original_title","original_language","overview","poster_path","release_date","adult","processed_at"))

def build_bridge(df):
    return (df.select("movie_id", F.explode("genre_ids").alias("genre_id"))
        .filter(F.col("genre_id").isNotNull()).distinct())

def build_fact(df):
    w = Window.partitionBy("movie_id","source_date").orderBy(F.col("extraction_ts").desc())
    return (df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
        .select("movie_id","source_date","popularity","vote_average","vote_count","extraction_ts","processed_at"))

def create_schema():
    pg_execute("""
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE TABLE IF NOT EXISTS silver.dim_genres (genre_id INTEGER PRIMARY KEY, genre_name VARCHAR(100) NOT NULL);
        CREATE TABLE IF NOT EXISTS silver.dim_movies (movie_id INTEGER PRIMARY KEY, title VARCHAR(500), original_title VARCHAR(500), original_language VARCHAR(10), overview TEXT, poster_path VARCHAR(300), release_date DATE, adult BOOLEAN, processed_at TIMESTAMP);
        CREATE TABLE IF NOT EXISTS silver.bridge_movie_genres (movie_id INTEGER NOT NULL, genre_id INTEGER NOT NULL, PRIMARY KEY (movie_id, genre_id));
        CREATE TABLE IF NOT EXISTS silver.fact_trending (movie_id INTEGER NOT NULL, source_date DATE NOT NULL, popularity FLOAT, vote_average FLOAT, vote_count INTEGER, extraction_ts TIMESTAMP, processed_at TIMESTAMP, PRIMARY KEY (movie_id, source_date));
    """)

def upsert_all():
    pg_execute("""
        INSERT INTO silver.dim_genres SELECT * FROM silver.dim_genres_staging
        ON CONFLICT (genre_id) DO UPDATE SET genre_name = EXCLUDED.genre_name;
        DROP TABLE IF EXISTS silver.dim_genres_staging;
    """)
    pg_execute("""
        INSERT INTO silver.dim_movies SELECT * FROM silver.dim_movies_staging
        ON CONFLICT (movie_id) DO UPDATE SET title=EXCLUDED.title, original_title=EXCLUDED.original_title,
        original_language=EXCLUDED.original_language, overview=EXCLUDED.overview,
        poster_path=EXCLUDED.poster_path, release_date=EXCLUDED.release_date,
        adult=EXCLUDED.adult, processed_at=EXCLUDED.processed_at;
        DROP TABLE IF EXISTS silver.dim_movies_staging;
    """)
    pg_execute("""
        INSERT INTO silver.bridge_movie_genres SELECT * FROM silver.bridge_movie_genres_staging
        ON CONFLICT (movie_id, genre_id) DO NOTHING;
        DROP TABLE IF EXISTS silver.bridge_movie_genres_staging;
    """)
    pg_execute("""
        INSERT INTO silver.fact_trending SELECT * FROM silver.fact_trending_staging
        ON CONFLICT (movie_id, source_date) DO UPDATE SET
        popularity=EXCLUDED.popularity, vote_average=EXCLUDED.vote_average,
        vote_count=EXCLUDED.vote_count, extraction_ts=EXCLUDED.extraction_ts,
        processed_at=EXCLUDED.processed_at;
        DROP TABLE IF EXISTS silver.fact_trending_staging;
    """)

def main():
    args = parse_args()
    source_date = args.source_date
    print(f"[silver_job] Iniciando job Bronze -> Silver (dt={source_date})")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_bronze = read_bronze(spark, source_date)
    df_clean  = clean(df_bronze)

    create_schema()

    write_staging(build_dim_genres(spark), "silver.dim_genres_staging")
    write_staging(build_dim_movies(df_clean), "silver.dim_movies_staging")
    write_staging(build_bridge(df_clean), "silver.bridge_movie_genres_staging")
    write_staging(build_fact(df_clean), "silver.fact_trending_staging")

    upsert_all()
    spark.stop()
    print(f"[silver_job] Job concluido! (dt={source_date})")

if __name__ == "__main__":
    main()
