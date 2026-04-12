import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, first

def run_spark_analysis(args):
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DriverBehaviorSummary") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    print(f"Reading cleaned data from {args.input}...")

    # 2. Read the cleaned CSV into Spark DataFrame
    df = spark.read.csv(args.input, header=True, inferSchema=True)

    # 3. Compute Metrics per driverID
    print("Computing metrics via Spark aggregation...")
    summary_df = df.groupBy("driverID").agg(
        first("carPlateNumber").alias("carPlateNumber"),
        _sum("isOverspeedFinished").alias("totalOverspeedCount"),
        _sum("isFatigueDriving").alias("totalFatigueCount"),
        _sum("overspeedTime").alias("totalOverspeedTimeSeconds"),
        _sum("neutralSlideTime").alias("totalNeutralSlideTimeSeconds")
    )
    summary_df = summary_df.na.fill(0)

    # 4. Save to Database
    print(f"Saving summary analysis to MySQL Database at {args.db_host}...")
    try:
        # ** Cloud Method (Spark Native JDBC) **
        # --packages mysql:mysql-connector-java:8.0.33
        jdbc_url = f"jdbc:mysql://{args.db_host}:{args.db_port}/{args.db_name}"
        summary_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "summary") \
            .option("user", args.db_user) \
            .option("password", args.db_pass) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("overwrite") \
            .save()
        print("Successfully saved summary records via Spark JDBC!")
    except Exception as e:
        print(f"Error saving to database via JDBC: {e}")
        # ** Local Fallback Method ** 
        print("Falling back to local Pandas + SQLAlchemy method...")
        import pandas as pd
        from sqlalchemy import create_engine
        
        engine = create_engine(f"mysql+pymysql://{args.db_user}:{args.db_pass}@{args.db_host}:{args.db_port}/{args.db_name}")
        pd_df = summary_df.toPandas()
        pd_df.to_sql('summary', con=engine, if_exists='replace', index=False)
        print("Local fallback saved successfully!")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Driving Behavior Spark Analyzer")
    parser.add_argument("--input", type=str, default="cleaned_data.csv", help="Input CSV path (S3 or local)")
    parser.add_argument("--db_host", type=str, help="Database Host")
    parser.add_argument("--db_port", type=str, default="3306", help="Database Port")
    parser.add_argument("--db_user", type=str, help="Database User")
    parser.add_argument("--db_pass", type=str, help="Database Password")
    parser.add_argument("--db_name", type=str, default="comp4442_gp", help="Database Name")

    args = parser.parse_args()

    # Load from local .env if args are not provided (for local testing)
    if not args.db_host:
        print("No args provided, loading environment variables from .env...")
        from dotenv import load_dotenv
        load_dotenv()
        args.db_host = os.getenv("DB_HOST", "127.0.0.1")
        args.db_port = os.getenv("DB_PORT", "3306")
        args.db_user = os.getenv("DB_USER", "root")
        args.db_pass = os.getenv("DB_PASSWORD", "your_password")
        args.db_name = os.getenv("DB_NAME", "comp4442_gp")
    
    if args.input == "cleaned_data.csv" and not os.path.exists(args.input):
        print(f"Error: {args.input} not found locally. Please specify an S3 input path or run Data Preprocessing first.")
    else:
        run_spark_analysis(args)