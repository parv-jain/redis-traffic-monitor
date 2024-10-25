import csv
from kafka import KafkaConsumer
from json import loads
import ssl
import base64
import tempfile
import os

# Kafka configuration
bootstrap_servers = ['']  # Replace with your Kafka broker(s)
topic_name = 'redis-queries'  # Replace with your topic name
group_id = 'csv_writer_group-1'

# Base64 encoded certificates and key
client_cert_base64 = ""  # Replace with your Base64 encoded client certificate
client_key_base64 = ""   # Replace with your Base64 encoded client key
ca_cert_base64 = ""      # Replace with your Base64 encoded CA certificate

# Function to create a temporary file with decoded content
def create_temp_file(base64_content):
    decoded_content = base64.b64decode(base64_content)
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(decoded_content)
    temp_file.close()
    return temp_file.name

# Create temporary files for certificates and key
client_cert_path = create_temp_file(client_cert_base64)
client_key_path = create_temp_file(client_key_base64)
ca_cert_path = create_temp_file(ca_cert_base64)

# SSL configuration
ssl_context = ssl.create_default_context()
ssl_context.load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)
ssl_context.load_verify_locations(ca_cert_path)

# CSV file configuration
csv_filename = 'kafka-data-2024-10-25.csv'

# Create Kafka consumer with SSL and batch size configuration
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    security_protocol='SSL',
    ssl_context=ssl_context,
    max_poll_records=500  # Fetch up to 500 messages at a time
)

# Open CSV file for buffered writing
with open(csv_filename, mode='w', newline='', buffering=1024*10) as file:  # Buffer set to 10KB
    writer = None
    buffer = []

    # Consume messages in batches
    for message in consumer:
        data = message.value
        
        # If this is the first message, initialize the CSV writer
        if writer is None:
            fieldnames = data.keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
        
        # Append data to buffer
        buffer.append(data)
        
        # Flush buffer to CSV periodically
        if len(buffer) >= 500:  # Adjust buffer size as needed
            writer.writerows(buffer)
            buffer = []  # Clear buffer after writing
        
    # Write any remaining messages in buffer to CSV
    if buffer:
        writer.writerows(buffer)
    
    print(f"Finished writing data to {csv_filename}")

# Close the consumer and clean up temporary files
consumer.close()
os.unlink(client_cert_path)
os.unlink(client_key_path)
os.unlink(ca_cert_path)
