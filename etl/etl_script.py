import subprocess
import time
import os

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e.output}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

# Wait for the source PostgreSQL to be ready
if not wait_for_postgres("source_postgres"):
    exit(1)

print("Starting ELT script...")

# Configuration for source and destination databases
source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

# Setting the environment variable for PostgreSQL password
os.environ['PGPASSWORD'] = source_config['password']

# Dump command for the source database
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

# Execute the dump command
try:
    subprocess.run(dump_command, check=True, capture_output=True)
except subprocess.CalledProcessError as e:
    print(f"pg_dump command failed with exit status {e.returncode}")
    print(f"Error output: {e.stderr.decode()}")
    exit(1)

# Setting the environment variable for PostgreSQL password for the destination database
os.environ['PGPASSWORD'] = destination_config['password']

# Load command for the destination database
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

# Execute the load command
try:
    subprocess.run(load_command, check=True, capture_output=True)
except subprocess.CalledProcessError as e:
    print(f"psql load command failed with exit status {e.returncode}")
    print(f"Error output: {e.stderr.decode()}")
    exit(1)

print("ELT script finished successfully!")
