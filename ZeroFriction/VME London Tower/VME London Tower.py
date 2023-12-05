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

name = 'VME London Tower'
basepath = f'C:/Users/jborr/OneDrive/Noven/Repos/python-scripts-dataminer/ZeroFriction/{name}/'

pd.options.plotting.backend = ["matplotlib","plotly"][1]
conn = http.client.HTTPSConnection("ems-noven.on.dataminer.services")
headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer MXHbrkL+HicuCCEiCJQ+W1qM/D9eHpsW78NzTJWFsyw=',
}

elements = ["Smappee", "Day ahead price"]
params = [    
    'Total Combined (Active Parts)',
    'Energy price',
]

instances = [    
    '',
    '144113.1', # Grid
    '144113.2', # Grid
    '144113.3', # Grid
    '144237.1', # CHP
    '144237.2', # CHP
    '144237.3', # CHP
    '144237.4', # ASHP
    '144237.6', # ASHP
    '144237.5', # ASHP
    '144237.7', # WSHP
    '144113.4', # EV
    '144113.5', # EV
    '144113.6', # EV
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

dfj.to_pickle(r'{basepath}{name}.pk')
# dfj = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/Dataminer python/Houtkant.pk')
#%% plotly
wide = pd.DataFrame()

dic = {
       'Energy price': 'DAP',
    'Total Combined (Active Parts) (144113.1)': 'Grid1',
    'Total Combined (Active Parts) (144113.2)': 'Grid2',
    'Total Combined (Active Parts) (144113.3)': 'Grid3',
    'Total Combined (Active Parts) (144237.1)': 'CHP1',
    'Total Combined (Active Parts) (144237.2)': 'CHP2',
    'Total Combined (Active Parts) (144237.3)': 'CHP3',
    'Total Combined (Active Parts) (144237.4)': 'ASHP1',
    'Total Combined (Active Parts) (144237.5)': 'ASHP2',
    'Total Combined (Active Parts) (144237.6)': 'ASHP3',
    'Total Combined (Active Parts) (144237.7)': 'WSHP',
    'Total Combined (Active Parts) (144113.4)': 'EV1',
    'Total Combined (Active Parts) (144113.5)': 'EV2',
    'Total Combined (Active Parts) (144113.6)': 'EV3',
    }

# To wide and rename
for i in dfj['PI'].unique():
    aux = dfj[dfj['PI'] == i].resample('1h').first().fillna(method='ffill')
    aux.rename(columns={'AverageValue': dic[i]}, inplace=True)  
    wide = pd.concat([wide, aux[dic[i]]], axis = 1)
    
for i in ['Grid', 'EV', 'CHP', 'ASHP']:
    wide[f'{i}'] = wide[f'{i}1'] + wide[f'{i}2'] + wide[f'{i}3']
    wide.drop([f'{i}1', f'{i}2', f'{i}3'], axis=1, inplace=True)

wide['VME_e0'] = wide['Grid'] + wide['CHP'] - wide['ASHP'] - wide['WSHP'] + wide['EV']
wide['VME_e'] = wide['VME_e0'].cummax()
wide['dVME_e'] = wide['VME_e'].diff()

wide[['Grid', 'EV', 'CHP', 'ASHP', 'WSHP', 'VME_e']].diff().plot().show('browser')

wide['VME_p'] = (wide['dVME_e'] * wide['DAP'].shift()).cumsum()/1000

wide[['VME_p', 'VME_e']].plot().show('browser')

wide.index = pd.to_datetime(wide.index, format='mixed')
wide = wide.sort_index()
# wide.plot().show('browser')

#%% Save files

for i, j in [['p', 'Final Price'], ['e', 'Energy']]:
    counter = f'{name} {j}'
    wide['Serial number'] = counter
    wide['Type'] =  'electricity'
    wide['Unit'] = 'kWh'
    wide['Value'] = wide[f'VME_{i}']
    wide['Datum local time'] = wide.index.tz_localize('UTC').tz_convert('Europe/Brussels').strftime('%d/%m/%Y %H:%M')
    wide['Datum UTC'] = wide.index.tz_localize('UTC').strftime('%d/%m/%Y %H:%M')
    
    out = wide
    out = out[['Serial number', 'Datum UTC', 'Datum local time', 'Value', 'Unit',  'Type']].copy()
    out['Value'] = out['Value'].round(2)
    out.to_csv(f'{basepath}{counter}.csv', sep=";", decimal=",", index=False)

#%% price vs energy (hour)
wide['dVME_e'] = wide['VME_e'].diff() 
wide['dVME_p'] = wide['VME_p'].diff()
fig = px.scatter(wide, x="dVME_e", y="dVME_p", hover_data=['VME_e', 'VME_p', wide.index])
fig.show('browser')    

