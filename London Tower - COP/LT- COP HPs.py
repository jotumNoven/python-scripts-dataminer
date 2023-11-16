# -*- coding: utf-8 -*-
#%% ⭐ Get data from dataminer (~10 min - 1 year)
import pandas as pd
import numpy as np
import plotly.express as px
import http.client
import json
from tqdm import tqdm
import datetime

headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer MXHbrkL+HicuCCEiCJQ+W1qM/D9eHpsW78NzTJWFsyw=',

}

params=[
       'Energy (Calorimeter)',
       'Total Consumption (Active Parts)',
       ]

instances = ['HP 1 CHP 1 SHW',
             'HP 1 CHP 1 CV',
             'ASHP 1',
             '144237.4',
             '144237.5',
             '144237.6',
             '144237.7',
             ]

stop = int(datetime.datetime.now().timestamp()*1000)
start = int(datetime.datetime.strptime('2023-11-05', "%Y-%m-%d").timestamp()*1000)
# stop = int(datetime.datetime.strptime('2023-02-05', "%Y-%m-%d").timestamp()*1000)
steps = 1
dfj = pd.DataFrame()
timeoutmsg = b'<html>\r\n<head><title>504 Gateway Time-out</title></head>\r\n<body>\r\n<center><h1>504 Gateway Time-out</h1></center>\r\n<hr><center>Microsoft-Azure-Application-Gateway/v2</center>\r\n</body>\r\n</html>\r\n'

for inst in tqdm(instances):
    print(f'\n{inst}:')
    i = start
    delta = int((stop-start)/steps)
    while i < stop:
        payload = {
          "element.names": bui,
          "parameter.names": params,
          "instances": [inst],
          "from": i,

          "to": int(min(i+delta, stop)),
        }
        
        startreq = datetime.datetime.now()
        conn = http.client.HTTPSConnection("ems-noven.on.dataminer.services")
        conn.request("POST", "/api/custom/trendj", str(payload), headers)
        res = conn.getresponse()
        data = res.read()   
        conn.close()
        
        if data == b'[]':
            i = i + delta
        else:
            endreq = datetime.datetime.now() 
            if data != timeoutmsg:
                df2 = pd.json_normalize(json.loads(data.decode('utf-8')), max_level=2)            
                if df2.columns.to_list()[0] != 'errors':
                    print(f'It took {(endreq - startreq).seconds} seconds. Received {df2.shape[0]} rows.')
                    dfj = pd.concat([dfj,df2])
                    i = i + delta
                else:
                    delta = delta/2
                    print(f'It took {(endreq - startreq).seconds} seconds. delta reduced to {delta/1000/60/60/24} days')
            else: 
                delta = delta/2
                print(f'It took {(endreq - startreq).seconds} seconds. delta reduced to {delta/1000/60/60/24} days')
                

dfj.index = pd.to_datetime(dfj['Time'], format='mixed')
dfj.index = dfj.index.tz_localize(None)
dfj['Time'] = dfj.index.tz_localize(None)
dfj.index.name = 't'

wide = pd.DataFrame()

dic = {
    'Energy (Calorimeter) (HP 1 CHP 1 SHW)': 'heat SHW',
    'Energy (Calorimeter) (HP 1 CHP 1 CV)': 'heat CV',
    'Energy (Calorimeter) (ASHP 1)': 'heat ASHP',
    'Total Consumption (Active Parts) (144237.4)': 'elec ASHP 1',
    'Total Consumption (Active Parts) (144237.5)': 'elec ASHP 2',
    'Total Consumption (Active Parts) (144237.6)': 'elec ASHP 3',
    'Total Consumption (Active Parts) (144237.7)': 'elec WSHP',
}

# To wide and rename
for i in dfj['PI'].unique():
    aux = dfj[dfj['PI'] == i].resample('1min').first().interpolate(method='linear')
    aux.rename(columns={'AverageValue': dic[i]}, inplace=True)  
    wide = pd.concat([wide, aux[dic[i]]], axis = 1)
 
# merge phases
for i in ['elec ASHP']:
    wide[f'{i}'] = wide[f'{i} 1'] + wide[f'{i} 2'] + wide[f'{i} 3'] 
    wide.drop([f'{i} 1', f'{i} 2', f'{i} 3'], axis=1, inplace=True)    
    wide[f'{i}'] = np.where(wide[f'{i}']==wide[f'{i}'].cummax(),wide[f'{i}'],np.nan)
    
# diff 
for i in wide.columns:
    wide[i] = np.where(wide[i] == wide[i].cummax(), wide[i], np.nan)
    wide[f'diff_{i}'] = wide[i].cummax().diff()

wide['COP ASHP'] = wide['diff_heat ASHP'] / (wide['diff_elec ASHP']*1000)
wide['COP WSHP'] = (wide['diff_heat SHW'].fillna(0) + wide['diff_heat CV'].fillna(0)) / (wide['diff_elec WSHP']*1000)
wide['COP SHW'] = wide['diff_heat SHW'] / (wide['diff_elec WSHP']*1000)
wide['COP CV'] = wide['diff_heat CV'] / (wide['diff_elec WSHP']*1000)

for i in wide.filter(regex='COP').columns:
    # lim = wide[i].quantile(0.99)
    # print(i, lim)
    wide[i] = np.where((wide[i]<20) & (wide[i]>0), wide[i] , np.nan) 

wide.index = pd.to_datetime(wide.index, format='mixed')
wide = wide.sort_index()
wide.filter(regex='COP').hist()

# convert wide to long
long = pd.DataFrame()
for i in wide.columns:
    aux = wide[[i]].copy()
    aux.rename(columns={i: 'ave'}, inplace=True)
    aux['param'] = i
    long = pd.concat([long, aux])

#%% ⭐Plotly 

import plotly.express as px
import plotly
long['hour'] = long.index.hour
long['weekday'] = long.index.weekday
long['month'] = long.index.month
long['quarter'] = long.index.quarter
long['week'] = long.index.isocalendar().week


dffig = long
# dffig = long[long['param'].str.find('COP')!=-1].copy()
# dffig['ave'] = np.where(dffig['ave'] > 0.5, dffig['ave'], np.nan)
# fig = px.scatter(dffig,
#               y ='ave',
#               # markers=True,
#               color='param',
#                marginal_y='box'
#               )
# fig.show('browser')
fig = px.line(dffig,
              y ='ave',
              # markers=True,
              color='param',
               # marginal_y='box'
              )

# add a secondary axis on the right side
# fig.update_yaxes(title_text="COP", secondary_y=False)
fig.update_yaxes(title_text="COP", secondary_y=True)

# add all traces which name contains 'COP' to secondary axis

for trace in fig.data:
    if 'COP' in trace.name:
        trace.yaxis = 'y2'

fig.show('browser')


fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1].replace(' - ', '<br>')))
# fig.layout.xaxis.range = [-0.35,0.35]
# fig.layout.yaxis.range = [-0.1,120]
# fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
# fig.layout.yaxis2.matches = 'y2'
# fig.layout.yaxis2.range = [50,90]
# fig.layout.yaxis3.matches = 'y3'
# fig.layout.yaxis3.range = [20,40]
# fig.write_html(r'C:/Users/jborr/OneDrive/Noven/Pieter/Effx3 by CHP,HP and season.html')
# fig.show('browser')