#%%
import pandas as pd

# %%
conso_sheets_list = ['Primary Energy Consumption',
                     'Oil Consumption - EJ',
                     'Gas Consumption - EJ',
                     'Coal Consumption - EJ',
                     'Nuclear Consumption - EJ',
                     'Hydro Consumption - EJ',
                     'Renewables Consumption - EJ',
                     'Solar Consumption - EJ',
                     'Wind Consumption - EJ',
                     'Geo Biomass Other - EJ',
                     'Biofuels Consumption - PJ']

def excel_to_dataframe(sheet:str):
  df = pd.read_excel('data_brut/bp-stats-review-2020-all-data.xlsx',
                    sheet_name=sheet,
                    skiprows=[0,1], skipfooter=8)
  df = df.drop(['2019.1','2008-18','2019.2'],axis=1)
  df = df.transpose()
  new_header = df.iloc[0]
  df = df[1:]
  df.columns = new_header
  df = df.dropna(1,how='all')
  df = df.dropna(0, how='all')
  return df

data_list = []
for sheet in conso_sheets_list:
  clean_df = excel_to_dataframe(sheet)
  data_list.append(clean_df)


# %%
df.index

df.isnull().sum()
# %%
df['Belarus']
# %%
dates = data_list[0].index
dates_df = pd.DataFrame(dates)
dates_df
# %%
locations = data_list[0].columns
locations_df = pd.DataFrame(locations)
locations_df
# %%
data_list[0]
# %%
types = ['Primary', 'Oil', 'Gas', 'Coal', 'Nuclear', 'Hydro', 'Renewables', 'Solar', 'Wind', 'Geo Biomass', 'Biofuel']
types_df = pd.DataFrame(types)
types_df
# %%
