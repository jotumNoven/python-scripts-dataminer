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

elements = ["Houtkant", "Smappee"]
params = [    
    'Total Active Energy Import (Electrical)',    
    'Total Combined (Active Parts)',    
]
instances = [    
    '116704.17', # (D) PV + CHP
    '116704.18', # (D) PV + CHP
    '116704.19', # (D) PV + CHP
    '116704.1', # (E) Resistor 1
    '116704.2', # (E) Resistor 1
    '116704.3', # (E) Resistor 1
    '116704.4', # (F) Resistor 2
    '116704.6', # (F) Resistor 2
    '116704.5', # (F) Resistor 2
    '116704.10', # (G) HP
    '116704.7', # (X) EV
    '116704.8', # (X) EV
    '116704.9', # (X) EV
    '104682.3', # (A) Net and EV
    '104682.1', # (A) Net and EV
    '104682.2', # (A) Net and EV
    '104682.4', # (B) Roots Extrapower
    '104682.6', # (B) Roots Extrapower
    '104682.5', # (B) Roots Extrapower
    'HP 4',
    ]

stop = int(datetime.datetime.now().timestamp()*1000)
start = int(datetime.datetime.strptime('2023-08-25', "%Y-%m-%d").timestamp()*1000)
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
     
CHP = instances[:3]
resistors = instances[3:9]
HP = instances[9:10]
EV = instances[10:13]
netEV = instances[13:16]
roots = instances[16:19]

dfj.rename(columns={
                    'Status': "status",
                    'Time': 'time',
                    'AverageValue': "ave",
                    'MinimumValue': "min",
                    'MaximumValue': "max",                   
                    }, inplace=True)    

dfj['time2'] = pd.to_datetime(pd.to_datetime(dfj['time'], format='%Y-%m-%dT%H:%M:%SZ', errors='ignore'), format='%Y-%m-%dT%H:%M:%S.%f', errors='ignore')

dfj.index = pd.to_datetime(dfj['time2'], format='mixed')
dfj.index = dfj.index.tz_localize(None)
dfj['time'] = dfj.index.tz_localize(None)
dfj.index.name = 't'

dfj['ave'] = np.where(dfj['status'].astype('int') < 0, np.nan, dfj['ave'])
dfj.sort_values(by=['E', 'P', 'time'], ascending=[True, True, True], inplace=True)

# dfj.to_pickle(r'C:/Users/jborr/OneDrive/Noven/Dataminer python/Houtkant.pk')
# dfj = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/Dataminer python/Houtkant.pk')

dic = {
    'Total Active Energy Import (Electrical) (HP 4)': 'Y_DM',
    'Total Combined (Active Parts) (104682.1)': 'A2',
    'Total Combined (Active Parts) (104682.2)': 'A3',
    'Total Combined (Active Parts) (104682.3)': 'A1',
    'Total Combined (Active Parts) (104682.4)': 'B1',
    'Total Combined (Active Parts) (104682.5)': 'B3',
    'Total Combined (Active Parts) (104682.6)': 'B2',
    'Total Combined (Active Parts) (116704.1)': 'E1',
    'Total Combined (Active Parts) (116704.10)': 'G',
    'Total Combined (Active Parts) (116704.17)': 'D1',
    'Total Combined (Active Parts) (116704.18)': 'D2',
    'Total Combined (Active Parts) (116704.19)': 'D3',
    'Total Combined (Active Parts) (116704.2)': 'E2',
    'Total Combined (Active Parts) (116704.3)': 'E3',
    'Total Combined (Active Parts) (116704.4)': 'F1',
    'Total Combined (Active Parts) (116704.5)': 'F3',
    'Total Combined (Active Parts) (116704.6)': 'F2',
    'Total Combined (Active Parts) (116704.7)': 'X1',
    'Total Combined (Active Parts) (116704.8)': 'X2',
    'Total Combined (Active Parts) (116704.9)': 'X3'}

wide = pd.DataFrame()

for i in dfj['PI'].unique():
    aux = dfj[dfj['PI'] == i].resample('1h').first().fillna(method='ffill')
    aux.rename(columns={'ave': dic[i]}, inplace=True)  
    wide = pd.concat([wide, aux[dic[i]]], axis = 1)

for i in ['A', 'B', 'D', 'F', 'E', 'X']:
    wide[f'{i}'] = wide[f'{i}1'] + wide[f'{i}2'] + wide[f'{i}3'] 
    wide.drop([f'{i}1', f'{i}2', f'{i}3'], axis=1, inplace=True)

for i in wide.columns:
    wide[f'diff{i}'] = np.where(wide[i].diff().abs() > 100, np.nan, wide[i].diff())
    
wide.index = pd.to_datetime(wide.index)   

long = pd.DataFrame()
for i in wide.columns:
    aux = wide[[i]].copy()
    aux['PI'] = i
    aux.rename(columns={i: 'ave'}, inplace=True)
    long = pd.concat([long, aux], axis=0)

# %%
wide['VME'] = (wide['A']
                + wide['B']*-1
                + wide['D']*1
                + wide['E']*-1
                + wide['F']*-1
                + wide['G']*-1
                + wide['X']*-1
                + wide['Y_DM']*-0.5)


cols = ['Serial number', 'Datum UTC', 'Datum local time', 'Value', 'Unit', 'Type']
wide = wide[(wide.index > '2023-09-30 21:59')].resample('h').first()
wide[cols[0]] = 'Houtkant VME'
wide[cols[1]] = wide.index.tz_localize('UTC').strftime('%d/%m/%Y %H:%M')
wide[cols[2]] = wide.index.tz_localize('UTC').tz_convert('Europe/Brussels').strftime('%d/%m/%Y %H:%M')
wide[cols[3]] = np.where(wide['VME'].diff(-1) > 0, wide['VME'].shift(1), wide['VME'])
wide[cols[3]] = np.where(wide[cols[3]].diff(-1) > 0, wide[cols[3]].shift(1), wide[cols[3]])
wide[cols[4]] = 'kWh'
wide[cols[5]] = 'electricity'

# wide['control'] = wide[['Y_DM', 'G', 'A', 'B', 'D', 'X']].min(axis=1)
# wide['VME'].diff().shift(-1).plot.line().show('browser')
# wide['VME'].diff().shift()

# wide[[cols[3], 'VME']].plot.line().show('browser')
# wide[cols[3]].diff().plot.line().show('browser')

# wide['VME'].plot.line().show('browser')
# wide['VME'].diff().plot.line().show('browser')

wide[cols].to_csv('C:/Users/jborr/OneDrive/Noven/Houtkant/VME.csv', index=False, sep=';', decimal=',', float_format='%.2f')

wide[cols[3]].diff().plot().show('browser')
