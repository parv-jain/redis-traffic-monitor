import pandas as pd
import numpy as np
import os
from datetime import datetime
import gc

def process_chunk(chunk):
    """Process a single chunk of data."""
    # Create a copy of the filtered DataFrame to avoid SettingWithCopyWarning
    processed_chunk = chunk[chunk['type'] == 'user'].copy()
    # Now safely modify the copied DataFrame
    processed_chunk.loc[:, 'duration_ms'] = processed_chunk['duration_in_ns'] / 1e6
    return processed_chunk

class DataProcessor:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.chunk_size = 1000000  # Adjust based on your system's RAM
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize aggregators
        self.top_by_size = []
        self.top_by_time = []
        self.operation_counts = {}
        self.operation_durations = {}
        self.operation_total_time = {}
        
    def update_top_n(self, current_top, new_entries, n, key):
        """Update top N items efficiently."""
        combined = current_top + new_entries
        return sorted(combined, key=lambda x: x[key], reverse=True)[:n]
    
    def update_operation_stats(self, chunk):
        """Update operation statistics incrementally."""
        for op, duration in zip(chunk['operation'], chunk['duration_ms']):
            if op not in self.operation_counts:
                self.operation_counts[op] = 0
                self.operation_durations[op] = []
                self.operation_total_time[op] = 0
            
            self.operation_counts[op] += 1
            self.operation_total_time[op] += duration
    
    def process_files(self, file_paths, n):
        """Process multiple CSV files in chunks."""
        for file_path in file_paths:
            print(f"Processing file: {file_path}")
            
            # Process the file in chunks
            chunk_iterator = pd.read_csv(
                file_path, 
                chunksize=self.chunk_size,
                dtype={
                    'type': 'category',
                    'operation': 'category',
                    'command': 'category',
                    'sender': 'category',
                    'receiver': 'category',
                    'duration_in_ns': 'float64',
                    'size_in_bytes': 'float64'
                }
            )
            
            for chunk_number, chunk in enumerate(chunk_iterator):
                print(f"Processing chunk {chunk_number}")
                
                # Process the chunk
                processed_chunk = process_chunk(chunk)
                
                # Update top N by size
                size_records = processed_chunk[['request', 'command', 'operation', 'start_time', 
                                             'duration_in_ns', 'size_in_bytes', 'sender', 
                                             'receiver', 'type', 'duration_ms']].to_dict('records')
                self.top_by_size = self.update_top_n(self.top_by_size, size_records, n, 'size_in_bytes')
                
                # Update top N by time
                # Using the same records for time analysis
                self.top_by_time = self.update_top_n(self.top_by_time, size_records, n, 'duration_ms')
                
                # Update operation statistics
                self.update_operation_stats(processed_chunk)
                
                # Force garbage collection
                del processed_chunk
                gc.collect()
    
    def save_results(self, n):
        """Save all results to CSV files."""
        # Save top N by size
        size_df = pd.DataFrame(self.top_by_size)
        size_df.to_csv(
            os.path.join(self.output_dir, f"top_{n}_requests_by_size.csv"), 
            index=False
        )
        del size_df
        
        # Save top N by time
        time_df = pd.DataFrame(self.top_by_time)
        time_df.to_csv(
            os.path.join(self.output_dir, f"top_{n}_requests_by_time.csv"), 
            index=False
        )
        del time_df
        
        # Save operation stats
        stats_data = [
            {
                'operation': op,
                'count': self.operation_counts[op],
                'duration_sum': self.operation_total_time[op],
                'duration_mean': self.operation_total_time[op] / self.operation_counts[op]
            }
            for op in self.operation_counts
        ]
        
        stats_df = pd.DataFrame(stats_data)
        stats_df = stats_df.sort_values('count', ascending=False)
        stats_df.to_csv(os.path.join(self.output_dir, "operation_stats.csv"), index=False)
        del stats_df
        
        # Save top N by throughput
        throughput_df = pd.DataFrame([
            {'operation': op, 'count': count}
            for op, count in sorted(self.operation_counts.items(), 
                                  key=lambda x: x[1], 
                                  reverse=True)[:n]
        ])
        throughput_df.to_csv(
            os.path.join(self.output_dir, f"top_{n}_by_throughput.csv"), 
            index=False
        )
        del throughput_df
        
        # Save top N by server time
        server_time_df = pd.DataFrame([
            {'operation': op, 'duration_ms': duration}
            for op, duration in sorted(self.operation_total_time.items(), 
                                     key=lambda x: x[1], 
                                     reverse=True)[:n]
        ])
        server_time_df.to_csv(
            os.path.join(self.output_dir, f"top_{n}_by_server_time.csv"), 
            index=False
        )
        del server_time_df
        gc.collect()

def main(file_paths, n, output_dir):
    start_time = datetime.now()
    print(f"Starting analysis at {start_time}")
    
    processor = DataProcessor(output_dir)
    processor.process_files(file_paths, n)
    processor.save_results(n)
    
    end_time = datetime.now()
    print(f"Analysis completed at {end_time}")
    print(f"Total processing time: {end_time - start_time}")

if __name__ == "__main__":
    file_paths = ['./kafka-data-2024-10-25.csv']
    n = 50
    output_dir = 'redis_analysis_results'
    main(file_paths, n, output_dir)