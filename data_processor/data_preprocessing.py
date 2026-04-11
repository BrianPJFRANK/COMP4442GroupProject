import pandas as pd
import glob
import os

def preprocess_data(input_dir):
    # 1. 定義標準結構 (19 columns)
    column_names = [
        "driverID", "carPlateNumber", "Latitude", "Longitude", "Speed",
        "Direction", "siteName", "Time", "isRapidlySpeedup", "isRapidlySlowdown",
        "isNeutralSlide", "isNeutralSlideFinished", "neutralSlideTime",
        "isOverspeed", "isOverspeedFinished", "overspeedTime",
        "isFatigueDriving", "isHthrottleStop", "isOilLeak"
    ]
    
    # 2. 搵晒所有 CSV files
    file_pattern = os.path.join(input_dir, "detail_record_*")
    files = glob.glob(file_pattern)
    files.sort()
    
    print(f"Found {len(files)} files to process.")
    all_data = []
    
    for f in files:
        print(f"Importing data from {os.path.basename(f)} into standard structure...")
        try:
            # 讀取 CSV，唔理佢有幾多 column，先讀入嚟
            df_raw = pd.read_csv(f, header=None, on_bad_lines='skip', engine='python')
            
            # 建立一個符合 19 column 標準結構嘅空白 DataFrame
            # 筆數 (rows) 同原本讀入嚟嘅一樣
            df_standard = pd.DataFrame(index=df_raw.index, columns=column_names)
            
            # 將 raw data 填入標準結構入面 (Import from daily records)
            # 就算原本只有 8 column，佢都只會填前 8 個，後面會自動係 NaN
            for i, col in enumerate(df_raw.columns):
                if i < len(column_names):
                    df_standard[column_names[i]] = df_raw[i]

            all_data.append(df_standard)
        except Exception as e:
            print(f"Warning: Failed to process {f}. Error: {e}")
            continue
    
    # 合併所有 DataFrame
    full_df = pd.concat(all_data, ignore_index=True)
    
    # 3. Data cleaning (處理 NaN 引發嘅問題)
    print("Cleaning data...")
    # Convert Time to datetime
    full_df['Time'] = pd.to_datetime(full_df['Time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    
    # Convert numeric columns to correct types
    numeric_cols = ["Latitude", "Longitude", "Speed", "Direction", "neutralSlideTime", "overspeedTime"]
    for col in numeric_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')
    
    # Convert flags to integers (handling potential NaN)
    flag_cols = [
        "isRapidlySpeedup", "isRapidlySlowdown", "isNeutralSlide", "isNeutralSlideFinished",
        "isOverspeed", "isOverspeedFinished", "isFatigueDriving", "isHthrottleStop", "isOilLeak"
    ]
    for col in flag_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce').fillna(0).astype(int)

    # Check for NaN and drop problematic rows if necessary
    initial_len = len(full_df)
    full_df = full_df.dropna(subset=['driverID', 'Time', 'Speed'])
    print(f"Rows after dropping NaNs in critical columns: {len(full_df)} (Dropped {initial_len - len(full_df)})")
    
    return full_df

if __name__ == "__main__":
    DATA_DIR = "detail-records"
    OUTPUT_FILE = "cleaned_data.csv"
    
    if os.path.exists(DATA_DIR):
        cleaned_df = preprocess_data(DATA_DIR)
        print(f"Saving cleaned data to {OUTPUT_FILE}...")
        cleaned_df.to_csv(OUTPUT_FILE, index=False)
        print("Done!")
    else:
        print(f"Error: Directory {DATA_DIR} not found.")
