import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
from pymongo.server_api import ServerApi


# ----------------------------
# Spark session
# ----------------------------
spark = SparkSession.builder.appName("FilmIndustryAnalysis").getOrCreate()


# ----------------------------
# File paths
# Update these to your own environment when running locally or in Colab
# ----------------------------
PATH_RATINGS = "/content/drive/My Drive/Datasets/title.ratings.csv"
PATH_BASICS = "/content/drive/My Drive/Datasets/title.basics.csv"
PATH_AKAS = "/content/drive/My Drive/Datasets/title.akas.csv"
PATH_NAME_BASICS = "/content/drive/My Drive/Datasets/name.basics.csv"
PATH_PRINCIPALS = "/content/drive/My Drive/Datasets/title.principals.csv"
OUTPUT_PARQUET = "/content/drive/My Drive/Datasets/final_merged_data.parquet"


# ----------------------------
# Load datasets
# ----------------------------
def load_data():
    df_ratings = spark.read.csv(PATH_RATINGS, header=True, inferSchema=True)
    df_basics = spark.read.csv(PATH_BASICS, header=True, inferSchema=True)
    df_akas = spark.read.csv(PATH_AKAS, header=True, inferSchema=True)
    df_name_basics = spark.read.csv(PATH_NAME_BASICS, header=True, inferSchema=True)
    df_principals = spark.read.csv(PATH_PRINCIPALS, header=True, inferSchema=True)

    return df_ratings, df_basics, df_akas, df_name_basics, df_principals


# ----------------------------
# Cleaning functions
# ----------------------------
def clean_ratings(df):
    return (
        df.na.replace("\\N", None)
        .dropDuplicates(["tconst"])
        .orderBy("tconst")
    )


def clean_basics(df):
    df = df.na.replace("\\N", None).filter("startYear IS NOT NULL")

    df = (
        df.withColumn("startYear", F.col("startYear").cast("int"))
          .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
    )

    avg_runtime = (
        df.groupBy("titleType")
          .agg(F.avg("runtimeMinutes").alias("avg_runtime"))
    )

    df = (
        df.join(F.broadcast(avg_runtime), on="titleType", how="left")
          .withColumn(
              "runtimeMinutes",
              F.when(
                  F.col("runtimeMinutes").isNull() | (F.col("runtimeMinutes") == 0),
                  F.col("avg_runtime")
              ).otherwise(F.col("runtimeMinutes"))
          )
          .drop("avg_runtime")
          .dropDuplicates(["tconst"])
          .orderBy("tconst")
    )

    return df


def clean_akas(df):
    df = df.replace("\\N", None).fillna({"region": "others"})
    df = df.filter(F.col("title").isNotNull() & F.col("titleId").isNotNull())

    df = df.withColumn(
        "null_count",
        F.expr("size(filter(array(*), x -> x IS NULL))")
    )

    window_spec = Window.partitionBy("titleId", "region").orderBy("null_count")

    return (
        df.withColumn("row_number", F.row_number().over(window_spec))
          .filter(F.col("row_number") == 1)
          .drop("null_count", "row_number")
    )


def clean_name_basics(df):
    header_row = df.collect()[0]
    new_columns = [str(cell) for cell in header_row]
    df = df.toDF(*new_columns)

    return (
        df.replace("\\N", None)
          .filter(
              (F.col("primaryName").isNotNull()) &
              (F.col("birthYear").isNotNull()) &
              (F.col("birthYear") >= 1800)
          )
          .withColumn("birthYear", F.col("birthYear").cast("int"))
          .drop("deathYear")
    )


def clean_principals(df):
    return (
        df.na.replace("\\N", None)
        .filter("tconst IS NOT NULL AND nconst IS NOT NULL AND category IS NOT NULL")
    )


# ----------------------------
# Merge datasets
# ----------------------------
def create_final_dataset(
    df_ratings_cleaned,
    df_basics_cleaned,
    df_akas_cleaned,
    df_name_basics_cleaned,
    df_principals_cleaned,
):
    df_ratings_cleaned.createOrReplaceTempView("ratings_cleaned")
    df_basics_cleaned.createOrReplaceTempView("basics_cleaned")
    df_akas_cleaned.createOrReplaceTempView("akas_cleaned")
    df_name_basics_cleaned.createOrReplaceTempView("name_basics_cleaned")
    df_principals_cleaned.createOrReplaceTempView("principals_cleaned")

    merged_principals_name_basics = spark.sql("""
        SELECT DISTINCT
            p.tconst,
            p.nconst,
            p.category,
            n.primaryName,
            n.birthYear
        FROM principals_cleaned p
        INNER JOIN name_basics_cleaned n
            ON p.nconst = n.nconst
    """)
    merged_principals_name_basics.createOrReplaceTempView("merged_principals_name_basics")

    merged_akas_title_basic = spark.sql("""
        SELECT DISTINCT
            a.titleId AS tconst,
            a.title,
            a.region,
            a.types,
            b.primaryTitle,
            b.genres,
            b.startYear,
            b.runtimeMinutes
        FROM akas_cleaned a
        INNER JOIN basics_cleaned b
            ON a.titleId = b.tconst
    """)
    merged_akas_title_basic.createOrReplaceTempView("merged_akas_title_basic")

    merged_akas_basic_ratings = spark.sql("""
        SELECT DISTINCT
            atb.*,
            r.averageRating,
            r.numVotes
        FROM merged_akas_title_basic atb
        LEFT JOIN ratings_cleaned r
            ON atb.tconst = r.tconst
    """)
    merged_akas_basic_ratings.createOrReplaceTempView("merged_akas_basic_ratings")

    final_merged_data = spark.sql("""
        SELECT
            pb.*,
            abr.title AS localizedTitle,
            abr.primaryTitle,
            abr.region,
            abr.genres,
            abr.types,
            abr.startYear,
            abr.runtimeMinutes,
            abr.averageRating,
            abr.numVotes
        FROM merged_principals_name_basics pb
        INNER JOIN merged_akas_basic_ratings abr
            ON pb.tconst = abr.tconst
    """)

    return final_merged_data.orderBy("tconst")


# ----------------------------
# Validation
# ----------------------------
def validate_dataset(df):
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]
    )
    null_counts.show()

    duplicates = df.count() - df.distinct().count()
    print(f"Number of duplicate rows: {duplicates}")

    df.printSchema()


# ----------------------------
# Save dataset
# ----------------------------
def save_dataset(df):
    df.write.parquet(OUTPUT_PARQUET, mode="overwrite")


# ----------------------------
# Example analysis functions
# ----------------------------
def top_genres_by_rating(df):
    df_genres = df.withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
    return (
        df_genres.groupBy("genre")
        .agg(F.avg("averageRating").alias("avg_rating"))
        .orderBy(F.col("avg_rating").desc())
    )


def ratings_by_year(df):
    return (
        df.groupBy("startYear")
        .agg(F.avg("averageRating").alias("avg_rating"))
        .orderBy("startYear")
    )


def profession_vs_rating(df):
    return (
        df.groupBy("category")
        .agg(
            F.avg("averageRating").alias("avg_rating"),
            F.avg("numVotes").alias("avg_votes")
        )
        .orderBy(F.col("avg_rating").desc())
    )


# ----------------------------
# MongoDB export
# ----------------------------
def export_to_mongodb(df):
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise ValueError("MONGODB_URI environment variable is not set.")

    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print("Connected to MongoDB.")

    database = client["movie_analysis"]
    collection = database["movie_dataset"]

    pandas_df = df.fillna(value="null").toPandas()
    records = pandas_df.to_dict(orient="records")

    result = collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")


# ----------------------------
# Main pipeline
# ----------------------------
def main():
    df_ratings, df_basics, df_akas, df_name_basics, df_principals = load_data()

    df_ratings_cleaned = clean_ratings(df_ratings)
    df_basics_cleaned = clean_basics(df_basics)
    df_akas_cleaned = clean_akas(df_akas)
    df_name_basics_cleaned = clean_name_basics(df_name_basics)
    df_principals_cleaned = clean_principals(df_principals)

    final_df = create_final_dataset(
        df_ratings_cleaned,
        df_basics_cleaned,
        df_akas_cleaned,
        df_name_basics_cleaned,
        df_principals_cleaned,
    )

    validate_dataset(final_df)
    save_dataset(final_df)

    print("Top genres by rating:")
    top_genres_by_rating(final_df).show(10)

    print("Average rating by year:")
    ratings_by_year(final_df).show(10)

    print("Profession vs rating:")
    profession_vs_rating(final_df).show(10)

    # Uncomment only when you want to export
    # export_to_mongodb(final_df)


if __name__ == "__main__":
    main()
