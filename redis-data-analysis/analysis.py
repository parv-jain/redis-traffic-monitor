import pandas as pd
import numpy as np
import os

def load_and_preprocess_data(file_paths):
    dfs = [pd.read_csv(file_path) for file_path in file_paths]
    df = pd.concat(dfs, ignore_index=True)
    df = df[df['type'] == 'user']
    df['duration_ms'] = df['duration_in_ns'] / 1e6
    return df

def top_n_requests_by_size(df, n):
    return df.nlargest(n, 'size_in_bytes')[['request', 'command', 'operation', 'start_time', 'duration_in_ns', 'size_in_bytes', 'sender', 'receiver', 'type']]

def top_n_requests_by_time(df, n):
    return df.nlargest(n, 'duration_ms')[['request', 'command', 'operation', 'start_time', 'duration_in_ns', 'size_in_bytes', 'sender', 'receiver', 'type']]

def operation_stats(df):
    stats = df.groupby('operation').agg({
        'duration_ms': ['sum', 'mean', 'count']
    })
    stats.columns = ['duration_sum', 'duration_mean', 'count']
    stats = stats.sort_values('count', ascending=False).reset_index()
    return stats

def top_n_operations_by_throughput(df, n):
    return df['operation'].value_counts().nlargest(n).reset_index()

def top_n_operations_by_server_time(df, n):
    return df.groupby('operation')['duration_ms'].sum().nlargest(n).reset_index()

def save_to_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f"Saved results to {filename}")

def main(file_paths, n, output_dir):
    df = load_and_preprocess_data(file_paths)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Top n by size
    top_size = top_n_requests_by_size(df, n)
    save_to_csv(top_size, os.path.join(output_dir, f"top_{n}_requests_by_size.csv"))
    
    # Top n by time
    top_time = top_n_requests_by_time(df, n)
    save_to_csv(top_time, os.path.join(output_dir, f"top_{n}_requests_by_time.csv"))
    
    # Operations stats
    stats = operation_stats(df)
    save_to_csv(stats, os.path.join(output_dir, "operation_stats.csv"))
    
    # Top n by throughput
    throughput = top_n_operations_by_throughput(df, n)
    save_to_csv(throughput, os.path.join(output_dir, f"top_{n}_by_throughput.csv"))
    
    # Top n by server time
    server_time = top_n_operations_by_server_time(df, n)
    save_to_csv(server_time, os.path.join(output_dir, f"top_{n}_by_server_time.csv"))

if __name__ == "__main__":
    file_paths = ['./kafka_data-2.csv', './kafka_data.csv']
    n = 50  # You can change this to any number you want
    output_dir = 'redis_analysis_results'  # Output directory name
    main(file_paths, n, output_dir)
