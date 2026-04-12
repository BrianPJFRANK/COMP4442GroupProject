import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class DBClient:
    def __init__(self):
        user = os.getenv("DB_USER", "root")
        password = os.getenv("DB_PASSWORD", "your_password")
        host = os.getenv("DB_HOST", "127.0.0.1")
        port = os.getenv("DB_PORT", "3306")
        db_name = os.getenv("DB_NAME", "driving_analysis")
        
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")
        
    def get_summary(self):
        try:
            # Query the database
            df = pd.read_sql("SELECT * FROM summary", self.engine)
            
            # Format to match API contract
            data_list = []
            for _, row in df.iterrows():
                data_list.append({
                    "driverID": str(row["driverID"]),
                    "carPlateNumber": str(row["carPlateNumber"]),
                    "totalOverspeedCount": int(row["totalOverspeedCount"] or 0),
                    "totalFatigueCount": int(row["totalFatigueCount"] or 0),
                    "totalOverspeedTimeSeconds": int(row["totalOverspeedTimeSeconds"] or 0),
                    "totalNeutralSlideTimeSeconds": int(row["totalNeutralSlideTimeSeconds"] or 0)
                })
            return {"status": "success", "data": data_list}
        except Exception as e:
            print(f"Error fetching summary from DB: {e}")
            return None
