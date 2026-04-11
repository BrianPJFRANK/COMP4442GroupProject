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
        self.simulated_start_time = self.raw_df['Time'].min()
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
            
        start_idx = max(current_idx, target_idx - 10)
            
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
                'isOverspeed': is_over
            })
            
        return {
            'status': 'success',
            'driverID': driver_id,
            'global_time': current_sim_time.strftime('%Y-%m-%d %H:%M:%S'),
            'speedData': speed_data,
            'warning':  warning
        }
