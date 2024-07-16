

import csv
import datetime as dt

import numpy as np
import pandas as pd
import time

# Sample data load
df_longitude = pd.read_csv('data/sample/longitudes_20230805.csv')
df_catalog = pd.read_csv('data/sample/satellitecatalog.csv')
df_networks = pd.read_csv('data/sample/networks_20230805.csv')

# Merge the satellites' longitude and catalog data into one file, using the NORAD ID as the matching key
df_merged = pd.merge(df_longitude, df_catalog, on = "NORAD")


# Create a dictionary for mapping SpaceTrack country names to ITU symbols
# Code from Thomas Roberts ITU-Assessment-Compliance-Monitor Github
SpaceTrackcountries_list = []
ITUcountrycodes_list = []
with open('data/sample/SpaceTrackcountries.csv') as f:
    reader = csv.reader(f, delimiter=",")
    for row in reader:
        if row[0] != "SpaceTrack Abbreviation":
            SpaceTrackcountries_list.append(row[0])
            row_list = [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30], row[31]]
            while('' in row_list):
                row_list.remove('')
            if len(row_list) == 0:
                row_list = ['n/a']
            ITUcountrycodes_list.append(row_list)
SpaceTrackcountries_dict = {SpaceTrackcountries_list[i]: ITUcountrycodes_list[i] for i in range(len(SpaceTrackcountries_list))}

start_time = time.perf_counter()

# --- Taaha's Code ---
"""
Loop through satellites in the longitude data;
Filter the networks that are within 1.0 degree;
Filter the satellite/network by country;
Save matched networks to a list
"""
matches = []

for i1, row in df_merged.iterrows():
    sat_long = row['Longitude']
    country = row['COUNTRY']

    itu_names = SpaceTrackcountries_dict[country]
    
    # Filter networks by country
    potential_networks = df_networks[(df_networks['ITU Administration'].str.strip().isin(itu_names))]
    potential_networks = potential_networks.reset_index(drop=True)
	
    # Replace any 'n/a' values in the network dataframe with zeros to make the logic in Nick's part work
    # .set_option ensures that calling 'fillna()' doesn't mess up any datatypes in potential_networks
    # pd.set_option('future.no_silent_downcasting', True)
    potential_networks = potential_networks.fillna(0)
    
    matched_networks = []
    for i2, network in potential_networks.iterrows():

        # Caluculate the longitudinal distance between the satellite and all potential networks
        # If this distance is greater than 180 degrees, adjust it so that it is within 0-180 degrees
        longitude_diff = abs(network['Longitude'] - sat_long)
        if longitude_diff > 180:
            longitude_diff = 360 - longitude_diff

        # --- Nick editing Taaha's Code ---
        '''
        Determine the compliant longitude difference range for this satellite based on the network's attributes:
        
        If a network is non-planned, check that the satellite is within 0.5 degrees
        If a network is planned, check that the satellite is within 0.1 degrees
        If a network was brought into use before 1987 AND has early stage filings before 1982, check within 1.0 degrees
        '''

        # Store the necessary values in variables to make the following logic cleaner
        planned_status = network['Planned or Non-Planned']

        # If a date value is stored as 'n/a', it was set to 0 earlier
        # We will now set these empty dates to 2000 so that they do not influence our compliance testing
        # For normal date values, store the year as an integer
        year_brought_into_use = int(network['Brought-into-Use Date'][:4])\
            if network['Brought-into-Use Date'] != 0 else 2000
           
        early_stage_file_year = int(network['Early-Stage Filing Date'][:4])\
            if network['Early-Stage Filing Date'] != 0 else 2000
        
        # If statement considering all three cases outlined above
        if (planned_status == 'Non-Planned' and longitude_diff <= 0.5)\
            or (planned_status == 'Planned' and longitude_diff <= 0.1)\
            or (year_brought_into_use < 1987 and early_stage_file_year < 1982 and longitude_diff <= 1.0):

                # Save this network to the list of matched networks
                matched_networks.append({
                    'Network Name': network['Network Name'],
                    'Longitude': network['Longitude'],
                    'ITU Administration': network['ITU Administration'],
                    'Planned Status': network['Planned or Non-Planned'],
                    'Early Stage Filing Date': network['Early-Stage Filing Date']
				})
        
    # Save the list of matched networks for this satellite, along with its identifying information, in the matches list
    matches.append({
        'NORAD': row['NORAD'],
        "SATNAME": row['SATNAME'],
        "COUNTRY": row['COUNTRY'],
        "LONGITUDE": sat_long,
        "MATCHED NETWORKS": matched_networks
    })


# Save the list of satellites and their matched networks as a text file
with open("results/matches.txt", 'w') as output_matches_file:
    for match in matches:
        norad = match['NORAD']
        country = match['COUNTRY']
        satname = match['SATNAME']
        longitude = match['LONGITUDE']
        output_matches_file.write(f'NORAD: {norad} | SATNAME: {satname} | COUNTRY: {country} | LONGITUDE: {longitude}\n')

        for network in match['MATCHED NETWORKS']:
            output_matches_file.write(f'{network}\n')

        # Space between each satellite for readability
        output_matches_file.write('\n\n')


# --- Nick's code ---
'''
Determine which satellites are compliant or not based on their network matches
Store each satellite identifying information along with its compliance status (Yes or No) in a text file
'''

# Make a copy of the list of satellites (and their attributes) to modify it
# We will add another row called "COMPLIANCE" for each satellite in this dataset
sat_data = df_merged

# Create an empty array to store the compliance status of each satellite
compliance = []

# Iterate through each satellite in the sat_data list
for index in range(len(sat_data)):

    # Check if this satellite is compliant
    # A satellite is compliant if it has at least one network match
    if len(matches[index]['MATCHED NETWORKS']) > 0:
        compliance.append('Yes')
    else:
        compliance.append('No')

# Add the compliance status for each satellite as a new column in sat_data
sat_data['COMPLIANCE'] = compliance

sat_data.to_csv('results/sat_data.csv', index = False)

with open('results/compliance.txt', 'w') as output_compliance_file:
    output_compliance_file.write(sat_data.to_string(index=False))

print('Done')


elapsed = time.perf_counter() - start_time
print(elapsed, 'seconds')