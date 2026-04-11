from fastapi import FastAPI, HTTPException

from fastapi.middleware.cors import CORSMiddleware

from contextlib import asynccontextmanager

from aws_client import AWSClient

from replay import ReplayModule

import os



# ?? Cache 變數

summary_cache = None

raw_cache = None

replay_engine = None



@asynccontextmanager

async def lifespan(app: FastAPI):

    # App startup: 將數????Memory (? Phase 3 ??)

    global summary_cache, raw_cache, replay_engine

    print("FastAPI is starting up... loading data into memory cache...")

    

    aws = AWSClient()

    

    # ?度你可以決定用 S3 定? Local

    # 建議?本?????用 local fallback

    if os.getenv('USE_S3', 'false').lower() == 'true':

        summary_cache = aws.load_summary_from_s3()

        raw_cache = aws.load_raw_csv_from_s3()

    else:

        # ?? data_processor ?面???
        summary_cache = aws.load_summary_local('../data_processor/summary.json')

        # ? Phase 4 ??: 載入三??? raw data ??memory，餵??Replay Engine

        raw_cache = aws.load_raw_local('../data_processor/cleaned_data.csv')

    

    if summary_cache is None:

        print("Warning: Failed to load summary cache!")

    else:

        print(f"Summary data loaded successfully: {len(summary_cache.get('data', []))} drivers.")



    if raw_cache is not None:

        # Initialise Replay Engine

        replay_engine = ReplayModule(raw_cache)

        print("Replay engine initialized successfully.")

    else:

        print("Warning: Failed to load raw CSV. Replay module will not work.")

        

    yield

    # App shutdown: 清? (如????

    print("FastAPI is shutting down...")

    summary_cache = None

    raw_cache = None



app = FastAPI(lifespan=lifespan)



# ?發?防止??(file:///, localhost) 讀 API ?現 CORS Error

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



# Phase 3: Summary API

@app.get("/api/summary")

async def get_summary():


    if summary_cache is None:

        raise HTTPException(status_code=503, detail="Summary data is not available yet.")

    return summary_cache



# Phase 4: Speed Monitoring endpoint fetching from Replay

@app.get("/api/speed/{driverID}")

async def get_speed_monitoring(driverID: str):


    global replay_engine

    if replay_engine is None:

        raise HTTPException(status_code=503, detail="Replay engine is not ready.")

    

    # 每次 Frontend call ?? API，??就?傳該?????來? 5 段???
    return replay_engine.get_next_batch(driverID)



