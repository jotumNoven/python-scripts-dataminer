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

elements = ["Imbalance", "Day ahead price"]
params = [    
    'Imbalance price',
    'Energy Price',
]

instances = [    
    '',  
    ]

stop = int(datetime.datetime.now().timestamp()*1000)
start = int(datetime.datetime.strptime('2023-01-01', "%Y-%m-%d").timestamp()*1000)
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
      'type:': 'real-time', 
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

# To wide and rename
for i in dfj['P'].unique():
    aux = dfj[dfj['P'] == i].resample('1min').first().fillna(method='ffill')
    aux.rename(columns={'ave': i}, inplace=True)  
    wide = pd.concat([wide, aux[i]], axis = 1)

# convert index to datetime
wide.index = pd.to_datetime(wide.index, format='%Y-%m-%dT%H:%M:%SZ', errors='ignore')
wide['Final price'] = wide['Energy Price'] - wide['Imbalance price'] - 60
wide['ispositive'] = np.where(wide['Final price'] > 0, 1, 0)
# calculate the percentage of positive prices y date
# wide['count'] = 1
wide['countpos'] = wide.groupby(wide.index.date)['ispositive'].transform('sum')
# wide['count'] = wide.groupby(wide.index.date)['count'].transform('sum')
wide['percpos'] = wide['countpos'] / 1440 * 100 # 1440 minutes in a day
wide['percpos'] = wide['percpos'].round(2)

# calculate ispositive, but only if it is positive for more than 15 minutes
wide['ispositive15'] = np.where(wide['Final price'] > 0, 1, 0)
wide['ispositive15'] = wide['ispositive15'].rolling(15).sum()
wide['ispositive15'] = np.where(wide['ispositive15'] == 15, 1, 0)
wide['countpos15'] = wide.groupby(wide.index.date)['ispositive15'].transform('sum')
# wide['count'] = wide.groupby(wide.index.date)['count'].transform('sum')
wide['percpos15'] = wide['countpos15'] / 1440 * 100 # 1440 minutes in a day
wide['percpos15'] = wide['percpos15'].round(2)


wide['percpos15'].describe()
# wide.plot().show('browser')

wide['hour'] = wide.index.hour
wide['day'] = wide.index.day
wide['month'] = wide.index.month
wide['year'] = wide.index.year
wide['weekday'] = wide.index.weekday
wide.groupby(by=wide.index.date).first()['percpos15'].plot().show('browser')
# create wide 2, with only the value of the first minute of the day and the last minute of the day. Keep all columns, use the index to filter. do not use groupby
wide2 = wide[((wide.index.minute == 0) & (wide.index.hour == 0)) | ((wide.index.minute == 59) & (wide.index.hour == 23))]
wide.filter(regex='percpos').plot().show('browser')


#%% 
# use plotly to plot the data

a = 'Maximum percentage of positive prices'
b = 'Percentage of positive prices<br><i>(positive prices for more than 15 minutes in a row)</i>)'
wide2[a] = wide2['percpos']
wide2[b] = wide2['percpos15']

fig = px.line(wide2, 
              # x = 'weekday',
               # y=['VME_VDAB','VME_VDAB2'],
                y=[a, b],
              # x = 'hour',
              # y = ['B', 'dA'],
             )

# draw a horizontal line at 10%, color it red, and make it dashed
fig.add_hline(y=10, line_dash="dot", annotation_text="10%", annotation_position="top right", line_color='green')
fig.add_hline(y=20, line_dash="dot", annotation_text="20%", annotation_position="top right", line_color='green')
# move the legend to the top left, and make it a bit smaller
fig.update_layout(legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.01,
    font=dict(
        size=16,
        color="black"
    )
))
# adjust the y axis range from 1 to 100
fig.update_yaxes(range=[0, 100])
fig.update_yaxes(tick0=0, dtick=10)
fig.update_layout(legend_title_text='Percentage of the day with positive prices')
fig.update_yaxes(title_text='<b>Percentage of the day with positive prices</b><br><i>(DAP - imbalance - 60) > 0 €/MWh</i>')

# adjust the tick to 1 day for the x axis
# fig.update_xaxes(
#     dtick="D",
#     tickformat="%d%\n%Y")

# draw a vertical line where there is a weekend
# get a list of all the weekends from wide2
mondays = wide2[wide2['weekday'] == 0].index.date
# convert the weekends to a list of strings
mondays = [str(i) for i in mondays]

saturdays = wide2[wide2['weekday'] == 5].index.date
# convert the weekends to a list of strings
saturdays = [str(i) for i in saturdays]

# drop duplicates in mondays and saturdays 
mondays = list(dict.fromkeys(mondays))
saturdays = list(dict.fromkeys(saturdays))
# deaw a vertical line for each monday
for i in range(len(saturdays)):
    fig.add_vrect(x0=saturdays[i], x1=mondays[i+1], annotation_text="", annotation_position="top left", fillcolor="green", opacity=0.25, line_width=0)
    # fig.add_vrect(x0=saturdays[i], line_dash="dot", annotation_text="Weekend", annotation_position="top left", line_color='red')

# fig.add_vrect(x0="2019-05-31", x1="2019-06-03", annotation_text="Weekend", annotation_position="top left", fillcolor="green", opacity=0.25, line_width=0)


fig.show('browser')
