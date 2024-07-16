import csv
import datetime as dt

import numpy as np
import pandas as pd

# Sample data load
df_longitude = pd.read_csv('roberts-data/longitudes_20230805.csv')
df_catalog = pd.read_csv('roberts-data/satellitecatalog.csv')
df_networks = pd.read_csv('roberts-data/networks_20230805.csv')
df_suspended = pd.read_csv('roberts-data/snl_suspended_20230805.csv')

# Merge the satellites' longitude and catalog data into one file, using the NORAD ID as the matching key
df_merged = pd.merge(df_longitude, df_catalog, on = "NORAD")

# Create a dictionary for mapping SpaceTrack country names to ITU symbols
# Code from Thomas Roberts ITU-Assessment-Compliance-Monitor Github
SpaceTrackcountries_list = []
ITUcountrycodes_list = []

with open('roberts-data/SpaceTrackcountries.csv') as f:
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

    '''
    Remove any networks that are currently inactive (have been totally suspended)
    from the list of potential networks
    '''
    suspension_data = df_suspended[(df_suspended['ADM'].str.strip().isin(itu_names))]
    suspension_data = suspension_data.reset_index(drop=True)
    # .set_option ensures that calling 'fillna()' doesn't mess up any datatypes in our dataframes
    pd.set_option('future.no_silent_downcasting', True)
    suspension_data = suspension_data.fillna('n/a')
    
    # todays_date = dt.datetime.today()
    # TO CHECK THOMAS ROBERT'S CODE, USE THE DATE OF DATA COLLECTION:
    checking_date = dt.datetime(2023, 8, 5)

    end_suspension_data = suspension_data['Date of resumption of operation']
    start_suspension_data = suspension_data['Date of suspension']

    suspended = []
    for i in range(len(potential_networks)):
        suspended.append('n/a')
        
        # Iterate through each row in suspension_data to determine which networks are inactive
        for date_index in range(len(suspension_data)):

            if potential_networks.at[i, 'Network Name'].strip() == suspension_data.at[date_index, 'Satellite&nbsp;Name'].strip():
            
                start_date_row = start_suspension_data[date_index]
                end_date_row = end_suspension_data[date_index]
        
                # Check if the network has a start suspension date
                if start_date_row != 'n/a':
                    start_suspend_date = dt.datetime.strptime(str(start_date_row), '%d.%m.%Y')
        
                    # Check if the network has an end suspension date
                    if end_date_row != 'n/a':
                        end_suspend_date = dt.datetime.strptime(str(end_date_row), '%d.%m.%Y')
        
                        # If the date of evaluation is after the start date of suspension and before or on the end date of suspension, the network is suspended
                        if start_suspend_date < checking_date and end_suspend_date >= checking_date \
                            and suspension_data.at[date_index, 'Type'].strip() == 'T':
                            
                            #print('Suspended', suspension_data.at[date_index, 'Type'])
                            # Remove the suspended network from the list of potential networks only if its suspension type is total ('T')
                            suspended[i] = 'Yes'
                            
                        else:
                            # Otherwise, the network is active and will remain in potential_networks
                            #print('Active')
                            suspended[i] = 'No'
                    else:
                        # If the network has no end suspension date, check if the date of evaluation is after the start date of suspension
                        if start_suspend_date < checking_date \
                            and suspension_data.at[date_index, 'Type'].strip() == 'T':
                          
                            #print('Suspended', suspension_data.at[date_index, 'Type'])
                            # If so, the network is currently suspended; remove it from potential_networks if it is a total suspension
                            suspended[i] = 'Yes'
                            
                        else:
                            #print('Active')
                            suspended[i] = 'No'
                else:
                    # If no suspension dates are provided, the network is active
                    #print('Active')
                    suspended[i] = 'No'
                    
                break

    # Remove any suspended networks from the list of potential networks
    potential_networks['Suspensions'] = suspended
    potential_networks = potential_networks[(potential_networks['Suspensions'].str.strip() != 'Yes')]
    potential_networks = potential_networks.reset_index(drop=True)
    # Replace any 'n/a' values in the network dataframe with zeros to make the logic below work
    potential_networks = potential_networks.fillna(0)

    # Begin matching process with the now filtered list of potential networks
    matched_networks = []
    for i2, network in potential_networks.iterrows():

        # Caluculate the longitudinal distance between the satellite and all potential networks
        # If this distance is greater than 180 degrees, adjust it so that it is within 0-180 degrees
        longitude_diff = abs(network['Longitude'] - sat_long)
        if longitude_diff > 180:
            longitude_diff = 360 - longitude_diff

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
                    'Early Stage Filing Date': network['Early-Stage Filing Date']})

    
    # Save the list of matched networks for this satellite, along with its identifying information, in the matches list
    matches.append({
        'NORAD': row['NORAD'],
        "SATNAME": row['SATNAME'],
        "COUNTRY": row['COUNTRY'],
        "LONGITUDE": sat_long,
        "MATCHED NETWORKS": matched_networks
    })


# Save the list of satellites and their matched networks as a text file
with open("matches.txt", 'w') as output_matches_file:
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


'''
Determine which satellites are compliant or not based on their network matches
Store each satellite's identifying information along with its compliance status (Yes or No) in a text file
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

# Save the compliance data to a text file called compliance.txt
with open('compliance.txt', 'w') as output_compliance_file:
    output_compliance_file.write(sat_data.to_string(index=False))

# Save sat_data for plotting purposes
sat_data.to_csv('sat_data.csv', sep = ',', index = False)

# End of Algorithm
print('Done')