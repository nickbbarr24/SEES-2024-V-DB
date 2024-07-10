

import pandas as pd

# Sample data load
df_longitude = pd.read_csv('data/sample/longitudes_20230805.csv')
df_catalog = pd.read_csv('data/sample/satellitecatalog.csv')
df_networks = pd.read_csv('data/sample/networks_20230805.csv')

print(df_networks.head())