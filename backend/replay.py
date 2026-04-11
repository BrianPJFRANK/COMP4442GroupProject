import pandas as pd

class ReplayModule:
    def __init__(self, raw_df):
        print("Initializing Replay Module...")
        self.raw_df = raw_df
        
        # 確保 Time 係 datetime 格式
        self.raw_df['Time'] = pd.to_datetime(self.raw_df['Time'], errors='coerce')
        
        # 按司機同時間排序，確保拿出來的資料係按時序
        self.raw_df = self.raw_df.sort_values(by=['driverID', 'Time'])
        
        # 將資料按司機分組，方便快速讀取
        self.driver_data = {
            driver: df.reset_index(drop=True) 
            for driver, df in self.raw_df.groupby('driverID')
        }
        
        # 記錄每位司機目前撥放到第幾條 (Pointer)
        self.driver_pointers = {driver: 0 for driver in self.driver_data.keys()}
        print(f"Replay Module Ready. Tracking {len(self.driver_data)} drivers.")

    def get_next_batch(self, driver_id, batch_size=5):
        """
        每次 Request，向後攞 batch_size 咁多條數據。
        """
        if driver_id not in self.driver_data:
            return {"status": "error", "message": f"Driver {driver_id} not found", "speedData": [], "warning": False}
        
        df = self.driver_data[driver_id]
        current_idx = self.driver_pointers[driver_id]
        
        # 如果播到最後，就 loop 返去開頭，方便 Demo
        if current_idx >= len(df):
            self.driver_pointers[driver_id] = 0
            current_idx = 0
            
        end_idx = min(current_idx + batch_size, len(df))
        batch = df.iloc[current_idx:end_idx]
        
        # 更新 Pointer
        self.driver_pointers[driver_id] = end_idx
        
        speed_data = []
        warning = False
        
        for _, row in batch.iterrows():
            # 判斷有冇超速 (isOverspeed == 1) 或者 Speed 大過 100 都可以當作 warning
            is_over = int(row.get('isOverspeed', 0))
            # 由於有些原始資料 isOverspeed 可能係 NaN/0，我地加多層保險：車速 > 100 亦觸發警告
            speed_val = float(row.get('Speed', 0))
            if is_over == 1 or speed_val > 100:
                is_over = 1
                warning = True
                
            speed_data.append({
                "time": row['Time'].strftime('%H:%M:%S') if pd.notnull(row['Time']) else "00:00:00",
                "speed": speed_val,
                "isOverspeed": is_over
            })
            
        return {
            "status": "success",
            "driverID": driver_id,
            "speedData": speed_data,
            "warning": warning
        }
