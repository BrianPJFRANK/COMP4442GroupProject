import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, first

def run_spark_analysis(input_csv, output_dummy=None):
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DriverBehaviorSummary") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    print(f"Reading cleaned data from {input_csv}...")

    # 2. Read the cleaned CSV into Spark DataFrame
    # Since we already cleaned it with pandas, types should be relatively stable
    df = spark.read.csv(input_csv, header=True, inferSchema=True)

    # 3. Compute Metrics per driverID
    # Requirements: car plate number, overspeed count, fatigue driving count, 
    # total overspeed time, total neutral slide time.
    print("Computing metrics via Spark aggregation...")
    
    summary_df = df.groupBy("driverID").agg(
        first("carPlateNumber").alias("carPlateNumber"),
        _sum("isOverspeedFinished").alias("totalOverspeedCount"),
        _sum("isFatigueDriving").alias("totalFatigueCount"),
        _sum("overspeedTime").alias("totalOverspeedTimeSeconds"),
        _sum("neutralSlideTime").alias("totalNeutralSlideTimeSeconds")
    )

    print("Saving summary analysis to MySQL Database...")
    try:
        import os
        from sqlalchemy import create_engine
        from dotenv import load_dotenv
        
        load_dotenv()
        user = os.getenv("DB_USER", "root")
        password = os.getenv("DB_PASSWORD", "your_password")
        host = os.getenv("DB_HOST", "127.0.0.1")
        port = os.getenv("DB_PORT", "3306")
        db_name = os.getenv("DB_NAME", "driving_analysis")
        
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")
        
        # Convert Spark DataFrame to Pandas and write to MySQL
        pd_df = summary_df.toPandas()
        
        # Fill any missing values with 0
        pd_df = pd_df.fillna(0)
        
        pd_df.to_sql('summary', con=engine, if_exists='replace', index=False)
        print(f"Successfully saved {len(pd_df)} summary records to database!")
    except Exception as e:
        print(f"Error saving to database: {e}")

    spark.stop()

if __name__ == "__main__":
    # In local testing, we use the cleaned_data.csv we just created
    INPUT_PATH = "cleaned_data.csv"
    
    if os.path.exists(INPUT_PATH):
        run_spark_analysis(INPUT_PATH, None)
    else:
        print(f"Error: {INPUT_PATH} not found. Please run Story 1 (data_preprocessing.py) first.")
