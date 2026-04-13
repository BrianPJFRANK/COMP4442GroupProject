import pandas as pd
import time
import datetime

class ReplayModule:
    def __init__(self, raw_df):
        print("Initializing Replay Module with Global Simulation Clock...")
        self.raw_df = raw_df
        self.raw_df['Time'] = pd.to_datetime(self.raw_df['Time'], errors='coerce')
        self.raw_df = self.raw_df.dropna(subset=['Time'])
        self.raw_df = self.raw_df.sort_values(by=['driverID', 'Time'])
        self.driver_data = {str(driver): df.reset_index(drop=True) for driver, df in self.raw_df.groupby('driverID')}
        self.driver_pointers = {driver: 0 for driver in self.driver_data.keys()}
        # Start at exactly 2017-01-01 08:00:00, which is the exact beginning of the dataset timeframes
        self.simulated_start_time = pd.to_datetime('2017-01-01 08:00:00')
        self.real_start_time = time.time()
        self.time_multiplier = 10  
        print(f"Replay Tracking {len(self.driver_data)} drivers.")

    def get_current_simulated_time(self):
        elapsed_real_seconds = time.time() - self.real_start_time
        elapsed_simulated_seconds = elapsed_real_seconds * self.time_multiplier
        return self.simulated_start_time + pd.Timedelta(seconds=elapsed_simulated_seconds)

    def get_next_batch(self, driver_id, batch_size=5):
        driver_id = str(driver_id)
        if driver_id not in self.driver_data:
            return {"status": "error", "message": f"Driver {driver_id} not found", "speedData": [], "warning": False}
        
        current_sim_time = self.get_current_simulated_time()
        df = self.driver_data[driver_id]
        current_idx = self.driver_pointers[driver_id]
        
        if current_idx >= len(df):
            self.real_start_time = time.time()
            for d in self.driver_pointers: self.driver_pointers[d] = 0
            current_idx = 0
            current_sim_time = self.get_current_simulated_time()
            
        target_idx = current_idx
        while target_idx < len(df) and df.at[target_idx, 'Time'] <= current_sim_time:
            target_idx += 1
            
        # Always return a sliding window of the last 20 records so the chart is always full
        start_idx = max(0, target_idx - 20)
            
        batch = df.iloc[start_idx:target_idx]
        self.driver_pointers[driver_id] = target_idx
        
        speed_data = []
        warning = False
        
        for _, row in batch.iterrows():
            is_over = int(row.get('isOverspeed', 0))
            speed_val = float(row.get('Speed', 0))
            if is_over == 1 or speed_val > 100:
                is_over = 1
                warning = True
                
            speed_data.append({
                'time': row['Time'].strftime('%H:%M:%S') if pd.notnull(row['Time']) else '00:00:00',
                'speed': speed_val,
                'isOverspeed': is_over,
                'isRapidlySlowdown': int(row.get('isRapidlySlowdown', 0) if not pd.isna(row.get('isRapidlySlowdown')) else 0),
                'isNeutralSlide': int(row.get('isNeutralSlide', 0) if not pd.isna(row.get('isNeutralSlide')) else 0),
                'isNeutralSlideFinished': int(row.get('isNeutralSlideFinished', 0) if not pd.isna(row.get('isNeutralSlideFinished')) else 0),
                'neutralSlideTime': float(row.get('neutralSlideTime', 0) if not pd.isna(row.get('neutralSlideTime')) else 0),
                'isOverspeedFinished': int(row.get('isOverspeedFinished', 0) if not pd.isna(row.get('isOverspeedFinished')) else 0),
                'overspeedTime': float(row.get('overspeedTime', 0) if not pd.isna(row.get('overspeedTime')) else 0),
                'isFatigueDriving': int(row.get('isFatigueDriving', 0) if not pd.isna(row.get('isFatigueDriving')) else 0),
                'isHthrottleStop': int(row.get('isHthrottleStop', 0) if not pd.isna(row.get('isHthrottleStop')) else 0),
                'isOilLeak': int(row.get('isOilLeak', 0) if not pd.isna(row.get('isOilLeak')) else 0)
            })
            
        return {
            'status': 'success',
            'driverID': driver_id,
            'global_time': current_sim_time.strftime('%Y-%m-%d %H:%M:%S'),
            'speedData': speed_data,
            'warning':  warning
        }
