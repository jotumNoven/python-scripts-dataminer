# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 10:33:28 2023

@author: jborr
"""

from itertools import product
import pandas as pd
import numpy as np
import plotly.express as px
import http.client
import json
from tqdm import tqdm
import datetime

pd.options.plotting.backend = ["matplotlib","plotly"][1]
conn = http.client.HTTPSConnection("ems-noven.on.dataminer.services")
headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer MXHbrkL+HicuCCEiCJQ+W1qM/D9eHpsW78NzTJWFsyw=',
}

elements = ["Houtkant", "Smappee"]
params = [    
    'Total Active Energy Import (Electrical)',
    'Total Combined (Active Parts)',    
    'Total Consumption (Active Parts)',    
    'Total Injection (Active Parts)' 
]

instances = [    
    '104667.7',  # Net
    '104667.8',  # Net
    '104667.9',  # Net
    '104667.10',  # Solar
    '104667.11',  # Solar
    '104667.12',  # Solar
    'HP 4', # Y/2 (from VME Houtkant) = B (Gio's document)
    ]

stop = int(datetime.datetime.now().timestamp()*1000)
start = int(datetime.datetime.strptime('2023-09-25', "%Y-%m-%d").timestamp()*1000)
steps = 1
delta = int((stop-start)/steps)
dfj = pd.DataFrame()
i = start

while i < stop:
    payload = {
      "element.names": elements,
      "parameter.names": params,
      "instances": instances,
      "from": i,
      "to": int(min(i+delta, stop)),
    }
    
    startreq = datetime.datetime.now()
    conn.request("POST", "/api/custom/trendj", str(payload), headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()
    endreq = datetime.datetime.now() 
    df2 = pd.json_normalize(json.loads(data.decode('utf-8')), max_level=2)
    if df2.columns.to_list()[0] != 'errors':
        print(f'It took {(endreq - startreq).seconds} seconds. Received {df2.shape[0]} rows.')
        dfj = pd.concat([dfj,df2])
        i = i + delta
    else:
        delta = delta/2
        print(f'It took {(endreq - startreq).seconds} seconds. delta reduced to {delta/1000/60/60/24} days')
     


dfj['time2'] = pd.to_datetime(pd.to_datetime(dfj['Time'], format='%Y-%m-%dT%H:%M:%SZ', errors='ignore'), format='%Y-%m-%dT%H:%M:%S.%f', errors='ignore')

dfj.index = pd.to_datetime(dfj['time2'], format='mixed')
dfj.index = dfj.index.tz_localize(None)
dfj['time'] = dfj.index.tz_localize(None)
dfj.index.name = 't'

dfj['ave'] = dfj['AverageValue']

dfj['ave'] = np.where(dfj['Status'].astype('int') < 0, np.nan, dfj['ave'])
dfj.sort_values(by=['E', 'P', 'time'], ascending=[True, True, True], inplace=True)

# dfj.to_pickle(r'C:/Users/jborr/OneDrive/Noven/Dataminer python/Houtkant.pk')
# dfj = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/Dataminer python/Houtkant.pk')
#%% plotly
wide = pd.DataFrame()

dic = {
    'Total Active Energy Import (Electrical) (HP 4)': 'Y_DM',
    'Total Combined (Active Parts) (104667.7)': 'Net1',
    'Total Combined (Active Parts) (104667.8)': 'Net2',
    'Total Combined (Active Parts) (104667.9)': 'Net3',
    'Total Combined (Active Parts) (104667.10)': 'Solar1',
    'Total Combined (Active Parts) (104667.11)': 'Solar2',
    'Total Combined (Active Parts) (104667.12)': 'Solar3',
    'Total Consumption (Active Parts) (104667.7)': 'Con1',
    'Total Consumption (Active Parts) (104667.8)': 'Con2',
    'Total Consumption (Active Parts) (104667.9)': 'Con3',
    'Total Consumption (Active Parts) (104667.10)': 'cSolar1',
    'Total Consumption (Active Parts) (104667.11)': 'cSolar2',
    'Total Consumption (Active Parts) (104667.12)': 'cSolar3',
    'Total Injection (Active Parts) (104667.7)': 'Inj1',
    'Total Injection (Active Parts) (104667.8)': 'Inj2',
    'Total Injection (Active Parts) (104667.9)': 'Inj3',
    'Total Injection (Active Parts) (104667.10)': 'iSolar1',
    'Total Injection (Active Parts) (104667.11)': 'iSolar2',
    'Total Injection (Active Parts) (104667.12)': 'iSolar3',
    }

# To wide and rename
for i in dfj['PI'].unique():
    aux = dfj[dfj['PI'] == i].resample('1h').first().fillna(method='ffill')
    aux.rename(columns={'ave': dic[i]}, inplace=True)  
    wide = pd.concat([wide, aux[dic[i]]], axis = 1)

# Merge phases and remove them
for i in ['Inj','Solar']:
    wide[f'{i}'] = wide[f'{i}1'] + wide[f'{i}2'] + wide[f'{i}3'] 
    wide.drop([f'{i}1', f'{i}2', f'{i}3'], axis=1, inplace=True)

for i in ['iSolar','cSolar', 'Con', 'Net']:
    wide.drop([f'{i}1', f'{i}2', f'{i}3'], axis=1, inplace=True)

wide['Y_DM'] = wide['Y_DM'].cummax()
wide['VME'] = (wide['Solar'] - wide['Inj'] + wide['Y_DM']/2).cummax()

for i in wide.columns:
    wide[f'diff{i}'] = np.where(wide[i].diff().abs() > 100, np.nan, wide[i].diff())

wide.index = pd.to_datetime(wide.index, format='mixed')
wide = wide.sort_index()
# wide.plot().show('browser')

#%%
wide['Serial number'] = 'VME Roots Myrnamacstraat'
wide['Type'] =  'electricity'
wide['Unit'] = 'kWh'
wide['Value'] = wide['VME']
wide['Datum local time'] = wide.index.tz_localize('UTC').tz_convert('Europe/Brussels').strftime('%d/%m/%Y %H:%M')
wide['Datum UTC'] = wide.index.tz_localize('UTC').strftime('%d/%m/%Y %H:%M')

out = wide
out = out[['Serial number', 'Datum UTC', 'Datum local time', 'Value', 'Unit',  'Type']].copy()
out['Value'] = out['Value'].round(2)
out.to_csv("C:/Users/jborr/OneDrive/Escritorio/VDAB/VME Roots Myrnamacstraat.csv", sep=";", decimal=",", index=False)

fig = px.line(wide, 
               # y=['VME_VDAB','VME_VDAB2'],
                y=['VME', 'diffVME', 'diffSolar', 'diffInj', 'diffY_DM'],
              # x = 'hour',
              # y = ['B', 'dA'],
             )

fig.show('browser')
# fig.write_html("C:/Users/jborr/OneDrive/Escritorio/VDAB/Roots.html")
# df2.to_csv("C:/Users/jborr/OneDrive/Escritorio/VDAB/VDAB_new.csv", sep=";", decimal=",", index=False)

