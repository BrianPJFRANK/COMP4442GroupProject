from fastapi import FastAPI, HTTPException

from fastapi.middleware.cors import CORSMiddleware

from contextlib import asynccontextmanager

from aws_client import AWSClient

from db_client import DBClient

from replay import ReplayModule

import os


raw_cache = None

replay_engine = None

db_client = None



@asynccontextmanager

async def lifespan(app: FastAPI):

    global raw_cache, replay_engine, db_client

    print("FastAPI is starting up... loading data into memory cache...")

    

    aws = AWSClient()

    try:

        db_client = DBClient()

        print("Database connection initialized.")

    except Exception as e:

        print("Error initializing DB:", e)


    if os.getenv('USE_S3', 'false').lower() == 'true':

        raw_cache = aws.load_raw_csv_from_s3()
        if raw_cache is None:
            print("Warning: S3 loading failed. Falling back to local CSV data.")
            raw_cache = aws.load_raw_local('../data_processor/cleaned_data.csv')

    else:

        raw_cache = aws.load_raw_local('../data_processor/cleaned_data.csv')  

    if raw_cache is not None:

        # Initialise Replay Engine

        replay_engine = ReplayModule(raw_cache)

        print("Replay engine initialized successfully.")

    else:

        print("Warning: Failed to load raw CSV. Replay module will not work.")      

    yield

    print("FastAPI is shutting down...")

    raw_cache = None

app = FastAPI(lifespan=lifespan)

app.add_middleware(

    CORSMiddleware,

    allow_origins=["*"],

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"],

)

@app.get("/")

def read_root():

    return {"message": "Driving Behavior Analysis API is online!"}

# Phase 3: Summary API (Now fetches dynamically from MySQL DB)

@app.get("/api/summary")

async def get_summary():

    global db_client
    aws = AWSClient()

    if db_client is None:
        print("Warning: Database client unavailable. Falling back to local summary JSON.")
        return aws.load_summary_local('../data_processor/summary.json')

    try:
        summary_data = db_client.get_summary()

        if summary_data is None:
            print("Warning: Database returned None. Falling back to local summary JSON.")
            return aws.load_summary_local('../data_processor/summary.json')

        return summary_data
    except Exception as e:
        print(f"Warning: Database query failed ({e}). Falling back to local summary JSON.")
        return aws.load_summary_local('../data_processor/summary.json')

# Phase 4: Speed Monitoring endpoint fetching from Replay

@app.get("/api/speed/{driverID}")

async def get_speed_monitoring(driverID: str):


    global replay_engine

    if replay_engine is None:

        raise HTTPException(status_code=503, detail="Replay engine is not ready.")

    return replay_engine.get_next_batch(driverID)