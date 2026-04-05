import json
import os
import time

import pandas as pd
from kafka import KafkaProducer

# Configuration (use KAFKA_TOPIC if another process also publishes to nyc_taxi_trips)
KAFKA_BOOTSTRAP_SERVERS = [os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "nyc_taxi_trips")
DATA_DIR = './data/'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_taxi_data():
    try:
        # Get all yellow_tripdata parquet files
        files = [f for f in os.listdir(DATA_DIR) if f.startswith('yellow_tripdata') and f.endswith('.parquet')]
        files.sort()

        if not files:
            print("No yellow_tripdata files found in data directory.")
            return

        print(f"Starting to produce data from {len(files)} files to Kafka topic: {TOPIC_NAME}")

        for filename in files:
            file_path = os.path.join(DATA_DIR, filename)
            print(f"Processing file: {filename}")
            
            # Read parquet file using pandas
            df = pd.read_parquet(file_path)
            
            # Convert timestamps to strings for JSON serialization
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Iterate over rows and send to Kafka
            for index, row in df.iterrows():
                message = row.to_dict()
                producer.send(TOPIC_NAME, value=message)
                
                # Optional: Send a small batch then sleep to simulate real-time
                if index % 100 == 0:
                    producer.flush()
                    print(f"Sent {index} records from {filename}...")
                    time.sleep(1) # Slow down for simulation

        producer.flush()
        print(f"Successfully finished producing data to {TOPIC_NAME}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_taxi_data()
