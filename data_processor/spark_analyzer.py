import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, first

def run_spark_analysis(input_csv, output_json):
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

    # 4. Format the output to match API Contract
    # { "status": "success", "data": [...] }
    rows = summary_df.collect()
    data_list = []
    for row in rows:
        data_list.append({
            "driverID": row["driverID"],
            "carPlateNumber": row["carPlateNumber"],
            "totalOverspeedCount": int(row["totalOverspeedCount"] or 0),
            "totalFatigueCount": int(row["totalFatigueCount"] or 0),
            "totalOverspeedTimeSeconds": int(row["totalOverspeedTimeSeconds"] or 0),
            "totalNeutralSlideTimeSeconds": int(row["totalNeutralSlideTimeSeconds"] or 0)
        })

    result = {
        "status": "success",
        "data": data_list
    }

    # 5. Save to JSON file
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
        
    print(f"Summary analysis completed. Results saved to {output_json}")
    spark.stop()

if __name__ == "__main__":
    # In local testing, we use the cleaned_data.csv we just created
    # In production (AWS EMR), this would point to S3 paths
    INPUT_PATH = "cleaned_data.csv"
    OUTPUT_PATH = "summary.json"
    
    if os.path.exists(INPUT_PATH):
        run_spark_analysis(INPUT_PATH, OUTPUT_PATH)
    else:
        print(f"Error: {INPUT_PATH} not found. Please run Story 1 (data_preprocessing.py) first.")
