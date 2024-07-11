import pandas as pd
import csv

# Sample data load
df_longitude = pd.read_csv('data/sample/longitudes_20230805.csv')
df_catalog = pd.read_csv('data/sample/satellitecatalog.csv')
df_networks = pd.read_csv('data/sample/networks_20230805.csv')

# Merge
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

# MAPS CATALOG TO NETWORK NAMES

# print(SpaceTrackcountries_dict)
# exit()


matches = []

"""
Loop through satellites in the longitude data;
Filter the networks that are within 1.0 degree;
Filter the satellite/network by country;
Save matched networks to a list
"""

for index, row in df_merged.iterrows():
    sat_long = row['Longitude']
    country = row['COUNTRY'].strip()

    itu_names = SpaceTrackcountries_dict[country]
    # print(itu_names)
    # break

    # Filter networks by country and longitude
    potential_networks = df_networks[(df_networks['ITU Administration'].str.strip().isin(itu_names))]

    matched_networks = []
    for i, network in potential_networks.iterrows():

        # Caluculate the longitudinal distance between the satellite and all potential networks
        # If this distance is greater than 180 degrees, adjust it so that it is within 0-180 degrees
        longitude_diff = abs(network['Longitude'] - sat_long)
        if longitude_diff > 180:
            longitude_diff = 360 - longitude_diff

        # If the satellite is within 1 degree of a network, add it to the list of matched networks
        if longitude_diff <= 0.5:
            matched_networks.append({
                'Network Name': network['Network Name'],
                'Longitude': network['Longitude'],
                'ITU Administration': network['ITU Administration']
            })

    # Save the list of matched networks for this satellite, along with its identifying information, 
    matches.append({
        'NORAD': row['NORAD'],
        "SATNAME": row['SATNAME'],
        "COUNTRY": row['COUNTRY'],
        "LONGITUDE": sat_long,
        "MATCHED NETWORKS": matched_networks
    })

# print(matches)

output_matches_file = open("results/matches.txt", 'w')

for match in matches:
    norad = match['NORAD']
    country = match['COUNTRY']
    satname = match['SATNAME']
    longitude = match['LONGITUDE']
    output_matches_file.write(f'NORAD: {norad} | SATNAME: {satname} | COUNTRY: {country} | LONGITUDE: {longitude}\n')

    for network in match['MATCHED NETWORKS']:
        output_matches_file.write(f'{network}\n')
    output_matches_file.write(f'\n\n')

print('done')
