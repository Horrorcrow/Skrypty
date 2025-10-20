import pandas as pd
from collections import defaultdict

types = defaultdict(lambda: str)

# Load the CSV file
df = pd.read_csv('5110d25e-9837-4824-b865-aa5218362a8e.csv', dtype=types, keep_default_na=False)

# Convert the 'Four Digit Model Year' column to datetime format
df['Four Digit Model Year'] = pd.to_numeric(df['Four Digit Model Year'])

# Filter the dates between 1997 and 2011
filtered_df_1997_2011 = df[(df['Four Digit Model Year'] >= 1997) & (df['Four Digit Model Year'] <= 2011)]

# Save the filtered data to a new CSV file with comma delimiters
filtered_df_1997_2011.to_csv('filtered_dates_1997_2011.csv', sep=',', index=False, quoting=1)

# Filter the dates between 2012 and 2026
filtered_df_2012_2026 = df[(df['Four Digit Model Year'] >= 2012) & (df['Four Digit Model Year'] <= 2026)]

# Save the filtered data to a new CSV file with comma delimiters
filtered_df_2012_2026.to_csv('filtered_dates_2012_2026.csv', sep=',', index=False, quoting=1)

print("Filtered dates have been saved")