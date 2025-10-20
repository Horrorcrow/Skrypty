import pandas as pd
import requests
import json
import time
import logging
from datetime import datetime
import csv

# Configure logging
logging.basicConfig(filename='wmi_processing.log', level=logging.INFO)

# Load the Excel file
file_path = 'C:\\Users\\I45560\\OneDrive - Verisk Analytics\\Desktop\\unique.xlsx'
try:
    # Read excel into DataFrame
    df = pd.read_excel(file_path, engine='openpyxl')
    print("Excel file loaded successfully.")
except Exception as e:
    print(f"Error loading Excel file: {e}")
    exit()

# Assuming the data is in the first column
wmi_list = df.iloc[:, 0].tolist()
total_wmis = len(wmi_list)
print(f"Total WMIs to process: {total_wmis}")

# Function to check the WMI_base CSV for existing data
def check_WMI_base(WMI):
    WMI_base_csv = 'C:\\Users\\I45560\\OneDrive - Verisk Analytics\\Desktop\\WMI_base.csv'
    
    with open(WMI_base_csv, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row['WMI'] == WMI:
                return row['Status'], row['VehicleType']
    return None, None

# Function to append new data to the WMI base CSV
def append_to_WMI_base(WMI, status, vehicle_type):
    WMI_base_csv = 'C:\\Users\\I45560\\OneDrive - Verisk Analytics\\Desktop\\WMI_base.csv'
    
    with open(WMI_base_csv, mode='a', newline='') as outfile:
        fieldnames = ['WMI', 'Status', 'Vehicle Type']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writerow({'WMI': WMI, 'Status': status, 'Vehicle Type': vehicle_type})
    
    # Log the information that the WMI has been added to the file
    logging.info(f"WMI {WMI} has been added to the file.")
    print(f"WMI {WMI} has been added to the file.")  # Print to console

# vPIC API endpoint
url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeWMI/"

# List to store the results and error logs
results = []
error_logs = []

def fetch_data(wmi):
    for _ in range(3):  # Retry up to 3 times
        try:
            print(wmi)
            response = requests.get(f"{url}{wmi}?format=json")
            if response.status_code == 429: # too many requests
                retry_after = int(response.headers.get("Retry-After", "5"))
                time.sleep(retry_after)
                raise Exception(f"Rate limiting hit {wmi} retry after {retry_after}")
            if response.status_code == 403: # blocked
                time.sleep(60 * 5)
                raise Exception(f"Blocked {wmi}")
            if response.status_code == 404:
                return None
            if response.status_code != 200: # not OK
                raise Exception(f"Invalid response status {wmi} {response.status_code}") 
            return response.json()
        except Exception as e:
            print(e)
            logging.error(f"Error fetching data for WMI {wmi}: {e}")
            error_logs.append({"WMI": wmi, "Error": str(e)})
            time.sleep(5)  # Wait 5 seconds before retrying
    return None

# Loop through each WMI and make a request
for index, wmi in enumerate(wmi_list):
    status, vehicle_type = check_WMI_base(wmi)
    
    if status is not None and vehicle_type is not None:
        results.append({"WMI": wmi, "Status": status, "VehicleType": vehicle_type, "Make": "", "ManufacturerName": "", "ParentCompanyName": ""})
        continue
    
    data = fetch_data(wmi)
    
    if data is None:
        results.append({"WMI": wmi, "Status": "Unknown", "VehicleType": "", "Make": "", "ManufacturerName": "", "ParentCompanyName": ""})
        continue
    
    if data['Count'] == 0:
        results.append({"WMI": wmi, "Status": "Not Found", "VehicleType": "", "Make": "", "ManufacturerName": "", "ParentCompanyName": ""})
    else:
        for result in data['Results']:
            vehicle_type = result.get('VehicleType', '')
            base_result = {
                "WMI": wmi,
                "Status": "Found",
                "VehicleType": vehicle_type,
                "Make": "",
                "ManufacturerName": "",
                "ParentCompanyName": ""
            }
            if vehicle_type in ['Incomplete Vehicle', 'Stripped Chassis']:
                base_result.update({
                    "Make": result.get("Make", ""),
                    "ManufacturerName": result.get("ManufacturerName", ""),
                    "ParentCompanyName": result.get("ParentCompanyName", ""),
                })
            results.append(base_result)
            append_to_WMI_base(wmi, base_result["Status"], base_result["VehicleType"])

    # Print progress update
    if (index + 1) % 50 == 0 or (index + 1) == total_wmis:
        print(f"Processed {index + 1} of {total_wmis}")
    
    # Add a delay to avoid API rate limiting
    time.sleep(0.1)  # 100 milliseconds delay

print("Finished processing WMIs.")

# Convert results to DataFrame and save to Excel file with multiple sheets
results_df = pd.DataFrame(results)
error_logs_df = pd.DataFrame(error_logs)

time_str = datetime.now().strftime('%Y%m%d%H%M')
with pd.ExcelWriter(f'wmi_results_{time_str}.xlsx') as writer:
    results_df.to_excel(writer, sheet_name='Results', index=False)
    error_logs_df.to_excel(writer, sheet_name='Error Logs', index=False)

print(f"The WMI data has been successfully written to wmi_results_{time_str}.xlsx")
