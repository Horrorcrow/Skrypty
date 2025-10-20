import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
import random
import csv
import os.path
import signal
import sys

cache_hit = 0
cache_miss = 0
starting_time = time.time()

def print_stats():
    global cache_hit, cache_miss
    print(f"Cache hit: {cache_hit}")
    print(f"Cache miss: {cache_miss}")
    process_time = time.time() - starting_time
    print(f"Total processing time: {process_time:.2f} seconds")

def signal_handler(sig, frame):
    print_stats()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Function to decode VIN using NHTSA API
def decode_vin(vin, model_year, max_retries=5, timeout=15, max_wait_time=10):
    url = f"https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/{vin}?format=xml&modelyear={model_year}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)  # Make a GET request to the API
            if response.status_code == 200:
                return response.text # Return the response text if the request is successful
        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}): {e}") # Print the error message if the request fails
        
        wait_time = min((2 ** attempt) + random.uniform(0, 1), max_wait_time) # Calculate wait time before retrying
        print(f"Retrying in {wait_time:.2f} seconds...")
        time.sleep(wait_time)
    
    print(f"Max retries reached for VIN: {vin}. Skipping...")
    return None

# Parse the XML data
def parse_xml(xml_data):
    root = ET.fromstring(xml_data)
    make = model = vehicle_type = None
    for variable in root.findall(".//DecodedVariable"):
        var_name = variable.find("Variable").text
        var_value = variable.find("Value").text if variable.find("Value") is not None else None
        if var_name == "Make":
            make = var_value
        elif var_name == "Model":
            model = var_value
        elif var_name == "Vehicle Type":
            vehicle_type = var_value
    return make, model, vehicle_type

# Function to check the models base CSV for existing data
def check_models_base(vin, model_year):
    global cache_hit, cache_miss
    models_base_csv = 'C:\\Users\\I45560\\OneDrive - Verisk Analytics\\Desktop\\Models_base.csv'
    
    with open(models_base_csv, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row['VIN'] == vin and row['Model Year'] == model_year:
                cache_hit += 1
                return row['Make'], row['Model'], row['Vehicle Type']
    cache_miss += 1
    return None, None, None

# Function to append new data to the models base CSV
def append_to_models_base(vin, model_year, make, model, vehicle_type):
    models_base_csv = 'C:\\Users\\I45560\\OneDrive - Verisk Analytics\\Desktop\\Models_base.csv'
    
    # Check if the VIN and Model Year combination already exists
    with open(models_base_csv, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row['VIN'] == vin and row['Model Year'] == model_year:
                print(f"Entry for VIN {vin} and Model Year {model_year} already exists. Skipping...")
                return
    
    # Append new data if it does not exist
    with open(models_base_csv, mode='a', newline='') as outfile:
        fieldnames = ['VIN', 'Model Year', 'Make', 'Model', 'Vehicle Type']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writerow({'VIN': vin, 'Model Year': model_year, 'Make': make, 'Model': model, 'Vehicle Type': vehicle_type})
        
        # Print and log the addition of new data to models base CSV
        log_message = f"Added to models base: VIN={vin}, Model Year={model_year}, Make={make}, Model={model}, Vehicle Type={vehicle_type}"
        print(log_message)
        with open("models_base_log.txt", mode='a') as log_file:
            log_file.write(log_message + "\n")

# Function to process input CSV and save the output CSV with vehicle information
def process_csv(input_csv, output_csv, start_vin=None, batch_size=2000, pause_time=60, pause_chunks=5):
    print("Processing input CSV")
    
    with open(input_csv, mode='r') as infile, open(output_csv, mode="a" if start_vin else "w", newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Make', 'Model', 'Vehicle Type']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        if start_vin is None:  # Write header if starting from the beginning
            writer.writeheader()
        
        for index, row in enumerate(reader):
            global cache_miss
            vin = row['Vin']
            model_year = row['Model Year']

            if start_vin is not None and vin == start_vin:
                start_vin = None
                print(f"Starting from row {index+1}")
                continue
            if start_vin is not None:
                continue

            # Check models base CSV first
            make, model, vehicle_type = check_models_base(vin, model_year)
            
            if make is None and model is None and vehicle_type is None:
                # If not found in models base, query NHTSA API
                xml_data = decode_vin(vin, model_year if model_year else "")
                time.sleep(random.uniform(0.4, 0.7))  # Slightly randomized short delay to prevent rate limiting
                if xml_data:
                    make, model, vehicle_type = parse_xml(xml_data)
                    # Append new data to models base
                    append_to_models_base(vin, model_year, make, model, vehicle_type)
            
            row['Make'] = make
            row['Model'] = model
            row['Vehicle Type'] = vehicle_type
            
            writer.writerow(row)
            
            # Print the processed VIN
            print(f"Processed VIN: {vin}")
                        
            if (cache_miss + 1) % 50 == 0:
                print(f"Processed online {cache_miss + 1} records so far.")
            
            if (cache_miss + 1) % batch_size == 0:
                print(f"Processed online {cache_miss + 1} VINs. Pausing gradually for {pause_time} seconds...")
                total_pause_time = 0
                while total_pause_time < pause_time:
                    small_pause = min(random.uniform(5, 15), pause_time - total_pause_time)
                    print(f"Pausing for {small_pause:.2f} seconds...")
                    time.sleep(small_pause)
                    total_pause_time += small_pause
    
    print(f"Processed CSV saved as {output_csv}")

# Function to read the last processed VIN from the output CSV
def read_last_vin(output_csv):
    if not os.path.isfile(output_csv):
        return None
    vin = None
    with open(output_csv, mode='r') as outfile:
        reader = csv.DictReader(outfile)
        for row in reader:
            vin = row['Vin']
    return vin

def get_yes_no_input(prompt):
    while True:
        user_input = input(prompt).strip().lower()
        if user_input in ['y', 'n']:
            return user_input
        print("Invalid input. Please enter 'y' or 'n'.")

input_csv = "C:/Users/I45560/OneDrive - Verisk Analytics/Desktop/main-env/vin_with_years_output.csv"
output_csv = "final list.csv"


start_vin = read_last_vin(output_csv)

if start_vin is not None:
    user_input = get_yes_no_input(f"Do you want to continue from VIN {start_vin}? (y/n)")
    if user_input == 'n':
        start_vin = None

process_csv(input_csv, output_csv, start_vin)
print_stats()
