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

pd.options.plotting.backend = "plotly"
conn = http.client.HTTPSConnection("ems-noven.on.dataminer.services")
headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer MXHbrkL+HicuCCEiCJQ+W1qM/D9eHpsW78NzTJWFsyw=',
}

elements = ["Arkenvest Appartementen CME"]
params = [    
    'Volume SWW (Watercounter)',    
    'Volume (Watercounter)',    
]

instances = [    
    'Kring_P_CV_VDAB', 
    'VDAB SWW aanvoer', 
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

wide = pd.DataFrame()

dic = {
    'Volume (Watercounter) (VDAB SWW aanvoer)': 'A',
    'Volume SWW (Watercounter) (Kring_P_CV_VDAB)': 'B'}

for i in dfj['PI'].unique():
    aux = dfj[dfj['PI'] == i].resample('1h').first().fillna(method='ffill')
    aux.rename(columns={'ave': dic[i]}, inplace=True)  
    wide = pd.concat([wide, aux[dic[i]]], axis = 1)


wide['VME_VDAB'] = wide['B'] - wide['A']
wide['dA'] =   wide['A'].diff().clip(lower=0).cumsum()+wide['A'].iloc[0]
wide['VME_VDAB_original'] = (wide['B'] - wide['dA']).cummax()

# wide['dVME_VDAB2'] = wide['VME_VDAB2'].diff()
# wide['VME_VDAB3'] = wide['VME_VDAB2']
wide.index = pd.to_datetime(wide.index, format='mixed')
wide['hour'] = wide.index.hour
wide = wide.sort_index()
#%%
p = ['VME_VDAB_original']
for i in list(range(100,0,-10))+[5, 1]:
    n = f'VME_VDAB <={i/100}m³/h'
    p.append(n)
    wide[n] = wide['VME_VDAB_original'].diff().clip(upper=i/100).cumsum()+wide['VME_VDAB_original'].min()
fig = px.line(wide, 
                # y=['VME_VDAB','VME_VDAB2'],
                # y=['VME_VDAB3'],
                y = p,
                # y=['VME_VDAB2', 'VME_VDAB3'],
              # x = 'hour',
               # y = ['B', 'dA','VME_VDAB2' ],
             )

# change the line 'VME_VDAB2' to a secondary axis
fig.update_traces(yaxis="y2", selector=dict(name="VME_VDAB2"))
# show the secondary axis on the right side
fig.update_layout(yaxis2=dict(anchor="x",  side="right", overlaying="y", position=1))
fig.show('browser')
fig.write_html("C:/Users/jborr/OneDrive/Escritorio/VDAB/VDAB.html")

#%%
wide['Serial number'] = 'Arkenvest - VDAB'
wide['Type'] =  'hottapwater'
wide['Unit'] = 'm3'
wide['Value'] = wide['VME_VDAB2']
wide['Datum local time'] = wide.index.tz_localize('UTC').tz_convert('Europe/Brussels').strftime('%d/%m/%Y %H:%M')
wide['Datum UTC'] = wide.index.tz_localize('UTC').strftime('%d/%m/%Y %H:%M')

out = wide[(wide['Datum local time'] >= '01/10/2023 00:00')
     & (wide['Datum local time'] < '08/11/2023 00:00')
     ]

out = wide
out = out[['Serial number', 'Datum UTC', 'Datum local time', 'Value', 'Unit',  'Type']].copy()
out['Value'] = out['Value'].round(2)
out.to_csv("C:/Users/jborr/OneDrive/Escritorio/VDAB/VDAB_new.csv", sep=";", decimal=",", index=False)

fig = px.line(out, 
               # y=['VME_VDAB','VME_VDAB2'],
                y=['Value'],
              # x = 'hour',
              # y = ['B', 'dA'],
             )

fig.show('browser')
# df2.to_csv("C:/Users/jborr/OneDrive/Escritorio/VDAB/VDAB_new.csv", sep=";", decimal=",", index=False)

