# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#%% ⭐ Get data from dataminer (~10 min - 1 year)
import pandas as pd
import numpy as np
import plotly.express as px
import http.client
import json
from tqdm import tqdm
import datetime

# training5.skyline.be
conn = http.client.HTTPSConnection("ems-noven.on.dataminer.services")
payload = {
  "protocol.name": "Noven Upgrade",
  "parameter.names":  ["[Number of active alarms]"],
  "instances": [""],
  "top": -1
}

headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer MXHbrkL+HicuCCEiCJQ+W1qM/D9eHpsW78NzTJWFsyw=',

}
conn.request("POST", "/api/custom/statusj", str(payload), headers)
res = conn.getresponse()
data = json.loads(res.read())
conn.close()
bui = []

for i in data:
    bui.append(i['ElementName'])
    

params=[
        "Electrical (CHP Efficiency)",
        "Thermal (CHP Efficiency)",
        "Overall (CHP Efficiency)"
       ]

stop = int(datetime.datetime.now().timestamp()*1000)
start = int(datetime.datetime.strptime('2023-02-03', "%Y-%m-%d").timestamp()*1000)
# stop = int(datetime.datetime.strptime('2023-02-05', "%Y-%m-%d").timestamp()*1000)
steps = 1
dfj = pd.DataFrame()
timeoutmsg = b'<html>\r\n<head><title>504 Gateway Time-out</title></head>\r\n<body>\r\n<center><h1>504 Gateway Time-out</h1></center>\r\n<hr><center>Microsoft-Azure-Application-Gateway/v2</center>\r\n</body>\r\n</html>\r\n'

for inst in tqdm(["CHP 1", "CHP 2", "CHP 3", "CHP 4"]):
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

chp = [['Mont Blanc (CHP 1)','2G','NIBE',20],
    ['Hermes (CHP 1)','Smartblock','NIBE',15],
    ['Bervoets (CHP 1)','Smartblock','ECOFOREST',50],
    ['Haut les Bois (CHP 1)','Smartblock','NIBE',50],
    ['Oxford (CHP 1)','2G','NIBE',20],
    ['Mimosa Chevrefeuille (CHP 1)','Smartblock','NIBE',15],
    ['Clos Ceres (CHP 1)','Smartblock','NIBE',15],
    ['Astrid (CHP 1)','2G','NIBE',50],
    ['Oxford Picadilly (CHP 1)','Smartblock','NIBE',33],
    ['Heliport 1 (CHP 1)','Smartblock','NIBE',50],
    ['RTB 2 - 3 (CHP 1)','Smartblock','NIBE',50],
    ['Quattuor (CHP 1)','Smartblock','NIBE',15],
    ['Eton (CHP 1)','2G','NIBE',20],
    ['Hauts de Saint Job (CHP 1)','2G','NIBE',20],
    ['Camargue (CHP 1)','Smartblock','0',50],
    ['Expo 4 (CHP 1)','Smartblock','0',50],
    ['Albatros (CHP 1)','Smartblock','NIBE',15],
    ['Heliport 2 - 3 (CHP 1)','Smartblock','NIBE',50],
    ['Moliere (CHP 1)','Smartblock','NIBE',15],
    ['Steyls (CHP 1)','2G','NIBE',20],
    ['Anciens Combattants (CHP 1)','2G','NIBE',50],
    ['Parc des Nations (CHP 1)','2G','ECOFOREST',20],
    ['PGA (CHP 1)','2G','0',20],
    ['Broqueville (CHP 1)','Smartblock','0',15],
    ['Bordet (CHP 1)','Smartblock','NIBE',33],
    ['Les Iris (CHP 1)','Smartblock','0',15],
    ['Les Francs (CHP 1)','2G','NIBE',50],
    ['Expo 3 (CHP 1)','2G','NIBE',20],
    ['Alba Regia (CHP 1)','Smartblock','Piggy',15],
    ['Saint Vincent (CHP 1)','Smartblock','NIBE',50],
    ['Piscine (CHP 1)','Smartblock','0',15],
    ['Charles de Lorraine (CHP 1)','Smartblock','0',15],
    ['Bouillot Darwin (CHP 1)','2G','NIBE',20],
    ['Up-Site (CHP 1)','Smartblock','ECOFOREST',50],
    ['Messidor (CHP 1)','2G','0',20],
    ['Belliard (CHP 1)','2G','0',20],
    # ['Belliard (CHP 1)','2G','ECOFOREST',20],
    ['Wiser 13-16 (CHP 1)','2G','NIBE',20],
    ['Liberateurs (CHP 1)','Smartblock','NIBE',15],
    ['Le Lac (CHP 1)','Smartblock','0',15],
    # ['Brusilia (CHP 1)','Smartblock','Piggy',50],
    ['Brusilia (CHP 1)','Smartblock', '0', 50],
    ['Le Leysin (CHP 1)','2G','NIBE',20],
    ['Parc Jean Monnet (CHP 1)','Smartblock','0',50],
    # ['Parc Jean Monnet (CHP 1)','Smartblock','NIBE',50],
    ['Parc du Lac (CHP 1)','Smartblock','NIBE',15],
    ['Up-Site (CHP 2)','Smartblock','ECOFOREST',50],
    ['Brusilia (CHP 2)','Smartblock','Piggy',50],
    ['Parc Jean Monnet (CHP 2)','Smartblock','0',50],
    # ['Parc Jean Monnet (CHP 2)','Smartblock','NIBE',50],
    ['Up-Site (CHP 3)','Smartblock','ECOFOREST',50],
    # ['Brusilia (CHP 3)','Smartblock','Piggy',50],
    ['Brusilia (CHP 3)','Smartblock', '0', 50],
    # ['Parc Jean Monnet (CHP 3)','Smartblock','NIBE',50],
    ['Parc Jean Monnet (CHP 3)','Smartblock','0',50],
    ['Up-Site (CHP 4)','Smartblock','ECOFOREST',33],
    ['Mont Blanc (Total)','2G','NIBE',20],
    ['Hermes (Total)','Smartblock','NIBE',15],
    ['Bervoets (Total)','Smartblock','ECOFOREST',50],
    ['Haut les Bois (Total)','Smartblock','NIBE',50],
    ['Oxford (Total)','2G','NIBE',20],
    ['Mimosa Chevrefeuille (Total)','Smartblock','NIBE',15],
    ['Clos Ceres (Total)','Smartblock','NIBE',15],
    ['Astrid (Total)','2G','NIBE',50],
    ['Oxford Picadilly (Total)','Smartblock','NIBE',33],
    ['Heliport 1 (Total)','Smartblock','NIBE',50],
    ['RTB 2 - 3 (Total)','Smartblock','NIBE',50],
    ['Quattuor (Total)','Smartblock','NIBE',15],
    ['Eton (Total)','2G','NIBE',20],
    ['Hauts de Saint Job (Total)','2G','NIBE',20],
    ['Camargue (Total)','Smartblock','0',50],
    ['Expo 4 (Total)','Smartblock','0',50],
    ['Albatros (Total)','Smartblock','NIBE',15],
    ['Heliport 2 - 3 (Total)','Smartblock','NIBE',50],
    ['Moliere (Total)','Smartblock','NIBE',15],
    ['Steyls (Total)','2G','NIBE',20],
    ['Anciens Combattants (Total)','2G','NIBE',50],
    ['Parc des Nations (Total)','2G','ECOFOREST',20],
    ['PGA (Total)','2G','0',20],
    ['Broqueville (Total)','Smartblock','0',15],
    ['Bordet (Total)','Smartblock','NIBE',33],
    ['Les Iris (Total)','Smartblock','0',15],
    ['Les Francs (Total)','2G','NIBE',50],
    ['Expo 3 (Total)','2G','NIBE',20],
    ['Alba Regia (Total)','Smartblock','Piggy',15],
    ['Saint Vincent (Total)','Smartblock','NIBE',50],
    ['Piscine (Total)','Smartblock','0',15],
    ['Charles de Lorraine (Total)','Smartblock','0',15],
    ['Bouillot Darwin (Total)','2G','NIBE',20],
    ['Up-Site (Total)','Smartblock','ECOFOREST',183],
    ['Messidor (Total)','2G','0',20],
    # ['Belliard (Total)','2G','ECOFOREST',20],
    ['Belliard (Total)','2G','0',20],
    ['Wiser 13-16 (Total)','2G','NIBE',20],
    ['Liberateurs (Total)','Smartblock','NIBE',15],
    ['Le Lac (Total)','Smartblock','0',15],
    ['Brusilia (Total)','Smartblock','Piggy',150],
    ['Le Leysin (Total)','2G','NIBE',20],
    ['Parc Jean Monnet (Total)','Smartblock','0',150],
    # ['Parc Jean Monnet (Total)','Smartblock','NIBE',150],
    ['Parc du Lac (Total)','Smartblock','NIBE',15]]


dfj['CHP'] = dfj['EI'].replace(list(zip(*chp))[0],list(zip(*chp))[1])
dfj['HP'] = dfj['EI'].replace(list(zip(*chp))[0],list(zip(*chp))[2])
dfj['Power'] = dfj['EI'].replace(list(zip(*chp))[0],list(zip(*chp))[3])
dfj['Status'] = dfj['Status'].astype('int')
dfj['CHP+HP'] = dfj['CHP'] + ' - ' + dfj['HP']
dfj['CHP+HP+Power'] = dfj['CHP'] + ' - ' + dfj['HP'] + ' - ' + dfj['Power'].astype('str')



# dfj.to_pickle(r'C:/Users/jborr/OneDrive/Noven/CHPeff.pk')

dfj['hour'] = dfj.index.hour
dfj['weekday'] = dfj.index.weekday
dfj['month'] = dfj.index.month
dfj['quarter'] = dfj.index.quarter
dfj['week'] = dfj.index.isocalendar().week
# dfj = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/CHPeff.pk')
# dfj.to_pickle(r'C:/Users/jborr/OneDrive/Noven/CHPeff.pk')

dffig = dfj[(dfj['AverageValue'] > 1)
          & (dfj['Power'] < 100)
          & (dfj['AverageValue'] < 150)
          & (dfj['Status'] > 0)]
dffig.to_pickle(r'C:/Users/jborr/OneDrive/Noven/CHPeff3.pk')
# dffig['ECHP'] = dffig['E'] + ' - ' + dffig['CHP']
# dffig2 = dffig.copy()
# dffig2['E'] = dffig2['CHP']
# dffig2['ECHP'] = dffig2['E']

# dffig = pd.concat([dffig, dffig2])

#%% ⭐Plotly 
import plotly.express as px
import plotly

dffig['season'] = dffig['month'].replace([1,2,3,4,5,6,7,8,9,10,11,12],['Winter','Winter','Spring','Spring','Spring','Summer','Summer','Summer','Autumn','Autumn','Autumn','Winter'])
y = 'Performance [%]'

dffig[y] = dffig['AverageValue'] 

color = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P'][4]
facet_col = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month', 'quarter', 'season'][-1]
facet_row = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month'][-2]
pattern_shape = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month'][-2]
# hover_name='Building'

dffig['n'] = dffig.groupby(by=['CHP','HP', 'Power', 'CHP+HP+Power','P', facet_row])['E'].transform('nunique')
dffig['b'] = dffig.groupby(by=['CHP','HP', 'Power', 'CHP+HP+Power','P', facet_row])['E'].transform(lambda x: ', '.join(x.unique()))
hover_name='b'

# dffig.sort_values(by=['quarter'], inplace=True)
fig = px.box(dffig.groupby(by=['CHP+HP+Power','P', 'month']).sample(frac=0.05, random_state=1).sort_values(by=['month']),
              y=y,
              # y='ave',lø øøfdhdfnkjhgkjjkhkhlll
              # x=x,
              # text_auto=True,
              # z= 'aveT',
                facet_col=facet_col,
                facet_row=facet_row,
                facet_row_spacing=0.01,
                facet_col_spacing=0.005,
                # pattern_shape=pattern_shape,
                # histnorm='probability density',
                # facet_col_wrap=7   ,
                # trendline =['ols', 'lowess', 'rolling', 'ewm', 'expanding'][0],
                # nbinsx=20, nbinsy=20, 
                # marginal_x="box",
                # marginal_y="histogram",
                hover_name=hover_name,
                # trendline_color_override="black",
                category_orders={'P':['Overall (CHP Efficiency)', 'Electrical (CHP Efficiency)', 'Thermal (CHP Efficiency)'],
                                 'CHP+HP+Power':['2G - 0 - 20', '2G - ECOFOREST - 20', '2G - NIBE - 20',
                                   '2G - NIBE - 50', 'Smartblock - 0 - 15', 'Smartblock - 0 - 50',
                                   'Smartblock - ECOFOREST - 33', 'Smartblock - ECOFOREST - 50',
                                   'Smartblock - NIBE - 15', 'Smartblock - NIBE - 33',
                                   'Smartblock - NIBE - 50', 'Smartblock - Piggy - 15',
                                   'Smartblock - Piggy - 50']},
                # hover_data={'Timestamp'},
                color=color,
                # color_continuous_scale= [plotly.colors.cyclical.Twilight, 'bluered'][1],
                # title=title
              )


fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1].replace(' - ', '<br>')))
fig.layout.xaxis.range = [-0.35,0.35]
fig.layout.yaxis.range = [-0.1,120]
# fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
# fig.layout.yaxis2.matches = 'y2'
# fig.layout.yaxis2.range = [50,90]
# fig.layout.yaxis3.matches = 'y3'
# fig.layout.yaxis3.range = [20,40]
fig.write_html(r'C:/Users/jborr/OneDrive/Noven/Pieter/Effx3 by CHP,HP and season.html')
fig.show('browser')
#%% Analysis excel
# dffig = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/CHPeff2.pk')

for i in ['month','season']:
    a = dffig.groupby(by=['CHP','HP', 'Power', 'CHP+HP+Power','P', i])['AverageValue'].describe()
    b = dffig.groupby(by=['CHP','HP', 'Power', 'CHP+HP+Power','P', i])['E'].unique()
    
    c = pd.concat([a,b], axis =1)
    c['E'] = c['E'].astype('str').str.replace("(\[')|('\])|\n",'', regex=True).str.replace("'.{1,3}'",', ', regex=True)
    # c.reset_index().to_excel(r'C:/Users/jborr/OneDrive/Noven/Pieter/Eff {i}.xlsx')
    
    d = c.pivot_table(index=['CHP','HP', 'Power', 'CHP+HP+Power',i], columns='P', values=['count', 'mean', '25%', '50%', '75%', 'E'], aggfunc='first')
    d.columns = d.columns.swaplevel(0, 1)
    d.sort_index(axis=1, level=0, inplace=True)
    d.reset_index().to_excel(f'C:/Users/jborr/OneDrive/Noven/Pieter/Eff {i}.xlsx')

#%%% 🔚🔚🔚END🔚🔚🔚
#%% Plotly 
y = ['AverageValue', 'month'][1]
color = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month', 'quarter'][4]
facet_col = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month', 'quarter','AverageValue'][-1]
facet_row = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month', 'quarter'][-3]
pattern_shape = ['CHP', 'HP', 'Power', 'CHP+HP','CHP+HP+Power', 'E', 'P','month', 'quarter'][-2]
# dffig.sort_values(by=['CHP'], inplace=True)

fig = px.box(dffig[dffig['P'] != 'Tsdfshermal (CHP Efficiency)'],
              y=y,
             
              # y='ave',lø øøfdhdfnkjhgkjjkhkhlll
              # x=x,
              # text_auto=True,
              # z= 'aveT',
                facet_row=facet_row,
                facet_col=facet_col,
                # facet_col_wrap=7   ,
                # trendline =['ols', 'lowess', 'rolling', 'ewm', 'expanding'][0],
                # nbinsx=20, nbinsy=20, 
                # marginal_x="box",
                # marginal_y="histogram",
                # hover_name=hover_name,
                # trendline_color_override="black",
                # hover_data={'Timestamp'},
                # pattern_shape=pattern_shape,
                color=color,
                # points  = False, 
                # box=True,
                # color_continuous_scale= [plotly.colors.cyclical.Twilight, 'bluered'][1],
                # title=title
              )

# fig.data[0].box=True
# for i in fig.data:
#     if 'Smartblock' in i.legendgroup:
#         i.marker.color = 'red'
#     else:
#         i.marker.color = 'blue'
        
# fig.layout.xaxis.range = [-0.35,0.35]
# fig.layout.yaxis2.matches = 'y2'
# fig.layout.yaxis2.range = [50,90]
# fig.layout.yaxis3.matches = 'y3'
# fig.layout.yaxis3.range = [20,40]
# fig.write_html("C:/Users/jborr/OneDrive/Noven/SHW/PVT/Effb.html")
fig.show('browser')

#%% test connection multiple instances
import http.client, pandas as pd
from tqdm import tqdm
import datetime
import json

headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer rNMMYlqBX0T+qezU+05olW7hsTX5gf1ZY9ChkRs9YyU=',
}

conn = http.client.HTTPSConnection("training5.skyline.be")

stop = int(datetime.datetime.now().timestamp()*1000)
start = int(datetime.datetime.strptime('2022-07-01', "%Y-%m-%d").timestamp()*1000)

elements = ['Smappee']
params = ["Consumption (Active Parts)"]
instances= ["81755.1","81755.2","81755.3","81755.4","81755.5","81755.6","81757.1","81757.2","81757.3","81759.1","81759.2","81759.3","81760.1","81760.2","81760.3","81762.1","81762.2","81762.3","81765.1","81765.2","81765.3","81766.1","81766.2","81766.3","81769.1","81769.2","81769.3","81771.1","81771.2","81771.3","81775.1","81775.2","81775.3","91934.1","91934.2","91934.3","91937.1","91937.2","91937.3","102224.1","102224.2","102224.3","102224.4","102224.5","102224.6","102236.1","102236.2","102236.3","102242.1","102242.2","102242.3","102243.1","102243.2","102243.3","102372.1","102372.2","102372.3","102372.4","102372.5","102372.6","102373.1","102373.2","102373.3","102373.4","102373.5","102373.6","102374.1","102374.2","102374.3","102374.4","102374.5","102374.6","102375.1","102375.2","102375.3","102375.4","102375.5","102375.6","104662.1","104662.2","104662.3","104662.4","104662.5","104662.6","104662.7","104662.8","104662.9","104662.10","104662.11","104662.12","104662.13","104662.14","104662.15","104662.16","104662.17","104662.18","104662.19","104662.20","104662.21","104662.22","104662.23","104662.24","104662.25","104662.26","104662.27","104662.28","104667.1","104667.2","104667.3","104667.4","104667.5","104667.6","104667.7","104667.8","104667.9","104667.10","104667.11","104667.12","104667.13","104667.14","104667.15","104667.16","104667.17","104667.18","104667.19","104667.20","104667.21","104667.22","104667.23","104667.24","104667.25","104667.26","104667.27","104667.28","104682.1","104682.2","104682.3","104682.4","104682.5","104682.6","104682.7","104682.8","104682.9","104682.10","104682.11","104682.12","104682.13","104682.14","104682.15","104682.16","104682.17","104682.18","104682.19","104682.20","104682.21","104682.22","104682.23","104682.24","104682.25","104682.26","104682.27","104682.28","106528.1","106528.2","106528.3","116704.1","116704.2","116704.3","116704.4","116704.5","116704.6","116704.7","116704.8","116704.9","116704.10","116704.11","116704.12","116704.13","116704.14","116704.15","116704.16","116704.17","116704.18","116704.19","116704.20","116704.21","116704.22","116704.23","116704.24","116704.25","116704.26","116704.27","116704.28","127864.1","127864.2","127864.3","127864.4","127864.5","127864.6","127864.7","127864.8","127864.9","127864.10","127864.11","127864.12","127864.13","127864.14","127864.15","127864.16","127864.17","127864.18","127864.19","127864.20","127864.21","127864.22","127864.23","127864.24","127864.25","127864.26","127864.27","127864.28","130499.1","130499.2","130499.3","130499.4","130499.5","130499.6","130499.7","130499.8","130499.9","130499.10","130499.11","130499.12","130499.13","130499.14","130499.15","130499.16","130499.17","130499.18","130499.19","130499.20","130499.21","130499.22","130499.23","130499.24","130499.25","130499.26","130499.27","130499.28"]

# i = 248
# a = range(len(instances)-1,0,-10)
a = range(1, len(instances),10)

for i in  tqdm(a):
    conn = http.client.HTTPSConnection("training5.skyline.be")
    payload = {
      "element.names": elements,
      "parameter.names": params,
      "instances": instances[:i],
      "type": "",
      "top": -1,
      "from": start,
      "to": stop,
    }
    try:
        startreq = datetime.datetime.now()
        conn.request("POST", "/api/custom/trendj", str(payload), headers)
        res = conn.getresponse()
        data = res.read()
        endreq = datetime.datetime.now() 
        df = pd.json_normalize(json.loads(data.decode('utf-8')), max_level=2)
        conn.close()
        if df.columns.to_list()[0] == 'errors':
            print(f'(response sends errors). It took {(endreq - startreq).seconds} seconds. Not working with {i} instances')
        else:
            print(f'It worked with {i} instances. It took {(endreq - startreq).seconds} seconds. Received {df.shape[0]} rows.')
            # print(df.describe())
    except:
        conn.close()
        print(f'(CannotSendRequest: Request-sent). Not working with {i} instances. It took {(datetime.datetime.now() - startreq).seconds} seconds.')
    

#%% ⭐ read pickle
%matplotlib qt5
import pandas as pd
from tqdm import tqdm
import numpy as np

dfj = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/CHPeff.pk')




parameters = [
    ['T Depart (SHW Circuits)', 20, 105],
    ['Flow (Calori Meters Heating)', 0, 100],
    ['T Depart (Calori Meters Heating)', 10, 105],
    ['T Retour (Calori Meters Heating)', 10, 105],
    ['Thermal Power (Calori Meters Heating)', -1, 10000],
    ['Total Thermal Energy (Calori Meters Heating)', 0, 99999999999999999999],
]

dfj2 = pd.DataFrame()
dfs = []
for i,j,k in tqdm(parameters):
    df2 = dfj[dfj['param']==i].copy()
    exclude = df2[df2['param']==i].groupby(by='bui')['ave'].sum()<1
    exclude = exclude[exclude == True].index
    df2['ave'] = np.where((df2['ave']>=j) & (df2['ave']<=k), df2['ave'], np.nan)    
    df2 = df2[~df2['bui'].isin(exclude)]
    # the difference is assigned to the next item, so shift one up to be correct
    df2['deltaMins'] = (df2['time'].diff().dt.days*24*60 + df2['time'].diff().dt.seconds/60).shift(-1)
    df2 = df2[(df2['deltaMins']>0) & (df2['deltaMins']<1450)]
    # df2 = df2.groupby([0]).resample('10min').mean(numeric_only=True)
    
    dfs.append(df2)
    # df2.index = df2.index.get_level_values(1)
    dfj2 = pd.concat([dfj2, df2])
#%% ⭐ Power - flow -Temp

import datetime

startreq = datetime.datetime.now()

excludeB = [
    'Brusilia',
    'Mimosa Chevrefeuille'
    ]

# Average power
dfp = dfs[5].copy()
dfp = dfp[(dfp['min'] != 0) & (~dfp['bui'].isin(excludeB))]
# Calculate the timestmap diff, but adssign it to the upper row, as it has the value to be kept until the next timestamp
# Before doing it, be MUST sort the columns, first by building and then by time. The result is equivalent to split the df by building we have also to sort it by time, then)
dfp.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dfp['dt'] = dfp.groupby(by='bui')['time'].diff(1).shift(-1).dt.days*24*60*60+dfp.groupby(by='bui')['time'].diff(1).shift(-1).dt.seconds
dfp['dQ'] = dfp.groupby(by='bui')['ave'].diff(1).shift(-1) 
dfp = dfp[(dfp['dQ'] >= 0)] # Ave is a cummulative value, always increasing, so the differences cannot be lower than zero
dfp['aveP'] = dfp['dQ']*3600/dfp['dt']
dfp['Q'] = dfp['ave']
dfp['sp'] = dfp['status']

# Average volume
dff = dfs[1].copy()
dff = dff[(~dff['bui'].isin(excludeB))]
# In this case, we will calculate the volume from the intant flow.  
dff.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dff['aveV'] = dff['ave']
dff['dt'] = dff.groupby(by='bui')['time'].diff(1).shift(-1).dt.days*24*60*60+dff.groupby(by='bui')['time'].diff(1).shift(-1).dt.seconds
dff['dV'] = dff['ave']*dff['dt']/3600 # We assume the flow is constant for the whole period dt
dff['V'] = dff.groupby(by='bui')['dV'].cumsum()
dff['sf'] = dff['status']

# Average Temperature
dft = dfs[0].copy()
dft= dft[(~dft['bui'].isin(excludeB))]
# In this case, we will calculate the average temperature, so we don't need to do anything
dft.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dft['aveT'] = dft['ave']
dft['st'] = dft['status']

# Average Power (alternative, it seems better)
dfp2 = dfs[4].copy()
dfp2 = dfp2[(~dfp2['bui'].isin(excludeB))]
# In this case, we will calculate the average temperature, so we don't need to do anything
dfp2.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dfp2['aveP2'] = dfp2['ave']
dfp2['sp2'] = dfp2['status']

# Resample everything
r = 'mean'
dt = ['1min', '5min', '10min', '15min', '30min', '1h', '12h', '1d'][5]
dfp = dfp.groupby(by='bui').resample(dt).agg({'aveP': r, 'Q': 'mean', 'sp': 'mean'})
dff = dff.groupby(by='bui').resample(dt).agg({'aveV': r, 'V': 'mean', 'sf': 'mean'})
dft = dft.groupby(by='bui').resample(dt).agg({'aveT': r, 'st': 'mean'})
dfp2 = dfp2.groupby(by='bui').resample(dt).agg({'aveP2': r, 'sp2': 'mean'})

result = pd.concat([dfp, dff, dft, dfp2], axis=1)
result['status'] = result[['sp', 'sf', 'st', 'sp2']].mean(axis=1)

# result = result[result['status'].isin([5.0,60.0])]
# result = result.dropna(subset=['status', 'aveP2', 'aveV'])
result.drop(columns=['sp', 'sf', 'st', 'sp2', 'aveP'], inplace=True)

result = result[['aveV', 'aveT', 'aveP2', 'Q', 'V', 'status']]  
result.rename(columns={
                    'aveV': "Flow",
                    'aveT': "Temp",
                    'aveP2': "Power",
                    'Q': 'Total heat', 
                    'V': 'Total volume', 
                    }, inplace=True)     


r = result.copy()
result = result.reset_index()


result.rename(columns={
                    'bui': "Building",
                    't': 'Timestamp'}, inplace=True)

   

result.to_csv(r'C:/Users/jborr/OneDrive/Noven/PVTlong.csv', sep = ',', decimal = '.')
a = pd.pivot(result, index='Timestamp', columns='Building')
a = a[a.columns.sortlevel('Building')[0]]
a.to_csv(r'C:/Users/jborr/OneDrive/Noven/PVTWide.csv', sep = ',', decimal = '.')

endreq = datetime.datetime.now()
print(f'It took {(endreq - startreq).seconds} seconds. Resample of {dt}')
# If we want to check:
# dff['VT2'] = dff.groupby(by='bui')['V'].cumsum()
# dff[['VT2','VT']].plot()


# a['r'] = a['VT2']/a['VT']
# a.groupby(by= 'bui')[['VT', 'VT2']].max()
# a.groupby(by= 'bui')['VT'].max()/a.groupby(by= 'bui')['VT2'].max()

# we can test the results
# test = pd.DataFrame()
# for i in dfp['bui'].unique():
#     t0 = dfp[dfp['bui'] == i].copy()
#     t0['dt'] = t0.time.diff(1).shift(-1).dt.days*24*60*60+t0.time.diff(1).shift(-1).dt.seconds    
#     test =  pd.concat([test, t0])   
# a = dfp.groupby(by='bui')['dt'].describe()  
# b = test.groupby(by='bui')['dt'].describe()  
# (a != b).sum().sum() #this must be zero


# a = dfp[((dfp['min'] == 0)|(dfp['min'] == 0).shift(1)|(dfp['min'] == 0).shift(-1))]

#######

# a=pd.DataFrame()
# a['r'] = dff.groupby(by='bui').resample('1h').agg({'ave': 'mean', 'V': 'mean'})['ave'].groupby('bui').sum()
# a['orig'] = dff.groupby('bui')['ave'].sum()
# a['diff']= a['r'] - a['orig']
# a['diffp']= a['diff']/a['r']

# dff.groupby(by='bui').resample('1h').agg({'ave': 'mean', 'V': 'mean'})['ave'].groupby('bui').cumsum().groupby('bui').plot()
# dff.groupby(by='bui').resample('1h').agg({'ave': 'mean', 'V': 'mean'})['V'].groupby('bui').plot()

#%% ⭐ 0. PV curve

import datetime

startreq = datetime.datetime.now()

excludeB = [
    'Brusilia',
    'Mimosa Chevrefeuille'
    ]

# Average power
dfp = dfs[5].copy()
dfp = dfp[(dfp['min'] != 0) & (~dfp['bui'].isin(excludeB))]
# Calculate the timestmap diff, but adssign it to the upper row, as it has the value to be kept until the next timestamp
# Before doing it, be MUST sort the columns, first by building and then by time. The result is equivalent to split the df by building we have also to sort it by time, then)
dfp.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dfp['dt'] = dfp.groupby(by='bui')['time'].diff(1).shift(-1).dt.days*24*60*60+dfp.groupby(by='bui')['time'].diff(1).shift(-1).dt.seconds
dfp['dQ'] = dfp.groupby(by='bui')['ave'].diff(1).shift(-1) 
dfp = dfp[(dfp['dQ'] >= 0)] # Ave is a cummulative value, always increasing, so the differences cannot be lower than zero
dfp['aveP'] = dfp['dQ']*3600/dfp['dt']
dfp['Q'] = dfp['ave']
dfp['sp'] = dfp['status']

# Average volume
dff = dfs[1].copy()
dff = dff[(~dff['bui'].isin(excludeB))]
# In this case, we will calculate the volume from the intant flow.  
dff.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dff['aveV'] = dff['ave']
dff['dt'] = dff.groupby(by='bui')['time'].diff(1).shift(-1).dt.days*24*60*60+dff.groupby(by='bui')['time'].diff(1).shift(-1).dt.seconds
dff['dV'] = dff['ave']*dff['dt']/3600 # We assume the flow is constant for the whole period dt
dff['V'] = dff.groupby(by='bui')['dV'].cumsum()
dff['sf'] = dff['status']

# Average Temperature
dft = dfs[0].copy()
dft= dft[(~dft['bui'].isin(excludeB))]
# In this case, we will calculate the average temperature, so we don't need to do anything
dft.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dft['aveT'] = dft['ave']
dft['st'] = dft['status']

# Average Power (alternative, it seems better)
dfp2 = dfs[4].copy()
dfp2 = dfp2[(~dfp2['bui'].isin(excludeB))]
# In this case, we will calculate the average temperature, so we don't need to do anything
dfp2.sort_values(by=['bui','time'], ascending=[True, True], inplace=True)
dfp2['aveP2'] = dfp2['ave']
dfp2['sp2'] = dfp2['status']

# Resample everything
r = 'first'
dt = ['1min', '5min', '10min', '15min', '30min', '1h', '12h', '1d'][1]
dfp = dfp.groupby(by='bui').resample(dt).agg({'aveP': r, 'Q': 'mean', 'sp': 'mean'})
dff = dff.groupby(by='bui').resample(dt).agg({'aveV': r, 'V': 'mean', 'sf': 'mean'})
dft = dft.groupby(by='bui').resample(dt).agg({'aveT': r, 'st': 'mean'})
dfp2 = dfp2.groupby(by='bui').resample(dt).agg({'aveP2': r, 'sp2': 'mean'})

result = pd.concat([dfp, dff, dft, dfp2], axis=1)
result['status'] = result[['sp', 'sf', 'st', 'sp2']].mean(axis=1)

# result = result[result['status'].isin([5.0,60.0])]
result = result.dropna(subset=['status', 'aveP2', 'aveV'])
result.drop(columns=['sp', 'sf', 'st', 'sp2', 'aveP'], inplace=True)

result = result[['aveV', 'aveT', 'aveP2', 'Q', 'V', 'status']]  
result.rename(columns={
                    'aveV': "Flow",
                    'aveT': "Temp",
                    'aveP2': "Power",
                    'Q': 'Total heat', 
                    'V': 'Total volume', 
                    }, inplace=True)     


r = result.copy()
# result = result.reset_index()


# result.rename(columns={
                    # 'bui': "Building",
                    # 't': 'Timestamp'}, inplace=True)
   

# result.to_csv(r'C:/Users/jborr/OneDrive/Noven/PVTlong.csv', sep = ',', decimal = '.')
# a = pd.pivot(result, index='Timestamp', columns='Building')
# a = a[a.columns.sortlevel('Building')[0]]
# a.to_csv(r'C:/Users/jborr/OneDrive/Noven/PVTWide.csv', sep = ',', decimal = '.')

endreq = datetime.datetime.now()
print(f'It took {(endreq - startreq).seconds} seconds. Resample of {dt}')
#%% Flow

# Tref = 50 °C. Scale flow. No Temp? -> no scale. No restrictions for colder temps
Tref = 50
Tsupply = 12 # 12 °C water from the street
Ttank = 55
ro_water = 1000 # kg/m³
factor = (Tref-Tsupply)*ro_water*4.18/3600 #directly in kWh using m³, h, and °C
factor_tank = (Ttank-Tref)*ro_water*4.18/3600  #kWh/m³/°C

# Flow is in m³/h. If we use hours, then it is the same as the volume
r['normFlow'] = np.where(r['Temp'].isna(), r['Flow'], r['Flow']*r['Temp']/Tref)
# r['normFlow'] = r['Flow']
# r.groupby(by='bui')['tempFlow'].rolling(2).sum().groupby(by='bui').plot(legend=True)
# r.groupby(by='bui')['Flow'].rolling(2).sum().groupby(by='bui').plot(legend=True)

rr = r.reset_index()
rr.index = rr['t']

dfpv = pd.DataFrame()

for j in rr['bui'].unique():
    df2 = pd.DataFrame()
    for i in range(1,24):
        a = rr[rr['bui']==j]
        df1 = pd.DataFrame({'bui': j, 
                            'aveNormFlow':a['normFlow'].rolling(window=i).sum().max()/i,
                            'normFlow':a['normFlow'].rolling(window=i).sum().max(),
                            'Flow':a['Flow'].rolling(window=i).sum().max(),
                            'window': i}, index =[i] )
        df2 = pd.concat([df2,df1])
    df2['diffFlow'] = df2['Flow'].diff()
    df2['diffNormFlow'] = df2['normFlow'].diff()
    dfpv = pd.concat([dfpv,df2])
    
for i in range(1,24):
    df1 = pd.DataFrame({'bui': 'Power', 
                        'aveNormFlow': i*50/factor,
                        'normFlow': i*50/factor,
                        'Flow':i*50/factor,
                        'window': i}, index =[i] )

    dfpv = pd.concat([dfpv,df1])

for j in [0.5,1,1.5,2,3,5,10,15]:
    for i in range(1,24):
        df1 = pd.DataFrame({'bui': f'Power+v{j}', 
                            'aveNormFlow': i*50/factor+factor_tank*j/i,
                            'normFlow': i*50/factor+factor_tank*j/i,
                            'Flow':i*50/factor+factor_tank*j/i,
                            'window': i}, index =[i] )
    
        dfpv = pd.concat([dfpv,df1])






dfpv['Energy [kWh]'] = dfpv['normFlow']*factor


        # df1 = pd.DataFrame(a.max()) # m³/h in i h

# for i in range(1,24):
    # r.reset_index().groupby(r.index.get_level_values('bui'))['normFlow'].rolling(window=i).sum()
    # a = r.groupby(r.index.get_level_values('bui'))['normFlow'].rolling(window=i).sum()
    # a.index = a.index.droplevel(0)
    
    # df1 = pd.DataFrame(a.groupby(r.index.get_level_values('bui')).max()) # m³/h in i h
    # df1.rename(columns={'Flow': f'w{i}'}, inplace=True)   
    # df1 = df1.set_index([df1.index, [i]*df1.shape[0]]).rename_axis(['bui','window'])
    # df1['aveNormFlow'] = df1['normFlow']/i # in m³/h 
    # df1['fiddFlow']
    # dfpv = pd.concat([dfpv,df1], axis=0)
    # dfpv.reset_index().pivot_table(index='window', columns='bui', values='Flow')
    # r[f'w{i}'] = (r['Flow'].rolling(window=i).sum())
    # r[r.index.get_level_values('bui')=='Anciens Combattants'][f'w{i}'].sort_values().plot(legend=True)    
    # (r[r.index.get_level_values('bui')=='Anciens Combattants'][f'w{i}']/i).sort_values().plot(legend=True)    
    # print(r[r.index.get_level_values('bui')=='Anciens Combattants'][f'w{i}'].max())



# dfpv['Energy [kWh]'] = dfpv['normFlow']*
    

    # if i > 1:
    #     d = (r[r.index.get_level_values('bui')=='Anciens Combattants'][f'w{i}'].sort_values()-r[r.index.get_level_values('bui')=='Anciens Combattants'][f'w{i-3}'].sort_values()).sum()
    #     print(i, d)

    # result[f'w{i}'] = result.groupby(by='Building')['Flow'].rolling(i).sum().groupby(by='Building')/i
    # (result.groupby(by='Building')['Flow'].rolling(i).sum()/i).reset_index()['Flow'].plot()
    # (result.groupby(by='Building')['Flow'].rolling(i).sum()/i).groupby(by='Building').plot(legend=True)
    
    # (r.groupby(by='Building')['Flow'].rolling(i).sum()/i)
#%% graph PV
import plotly.express as px
import plotly

dfig =  dfpv
fig = px.line(dfig,
              y=['normFlow', 'aveNormFlow','diffFlow','diffNormFlow', 'Energy [kWh]' ][0],
              # y='ave',lø øøfdhdfnkjhgkjjkhkhlll
              x='window',
              # points='x',
              markers=True,
              # text_auto=True,
              # z= 'aveT',
                # facet_row=facet_row,
                # facet_col='bui',1
                # facet_col_wrap=7   ,
                # trendline =['ols', 'lowess', 'rolling', 'ewm', 'expanding'][0],
                # nbinsx=20, nbinsy=20, 
                # marginal_x="box",
                # marginal_y="histogram",
                # hover_name=hover_name,
                # trendline_color_override="black",
                # hover_data={'Timestamp'},
                color='bui',
                # color_continuous_scale= [plotly.colors.cyclical.Twilight, 'bluered'][1],
                # title=title
              )

# l =[
#     [24, 24*50/factor, '50 kW'],
#     [24, 24*100/factor, '100 kW'],
#     [24, 24*2000/factor, '2000 kW'],
#     ]

# axis = []
# for i in fig.data:
#     if [i.xaxis, i.yaxis]  not in axis:
#         axis.append([i.xaxis, i.yaxis])


# subplot = px.line(y=[0,1],
#               x=[0,1],
#               markers=True,
#               )
# subplot = subplot.data[0]

# # fig.add_trace(subplot)
# for x, y in axis:
#     for x1,y1,t in l:
#         subplot.x=[0,x1]
#         subplot.y=[0,y1]
#         subplot.line.color= 'olive'
#         subplot.hovertemplate = t
#         subplot.name = t
#         subplot.xaxis = x
#         subplot.showlegend = True
#         subplot.yaxis = y
#         subplot.legendgroup = t
#         fig.add_trace(subplot)

fig.show('browser')
#%% ⭐📊graphs
import plotly.express as px
import plotly

# p = result.resample('h').mean()
p = result[
    ((result['Temp']>10) & (result['Temp']<100))   
    # & (result['status'] == 60.0)
    # & ((result['aveP']>0) & (result['aveP']<500))
    # & ((result['aveV']>0) & (result['aveV']<5))
    # & ((result['aveP2']>0) & (result['aveP2']<100))    
    ]
p = p.reset_index()
p['tt'] = (p['Timestamp']-p['Timestamp'].min()).dt.seconds
ty = ['str', 'int'][0] 
p['h'] = p['Timestamp'].dt.hour.astype(ty)
p['m'] = p['Timestamp'].dt.month.astype(ty)
p['w'] = p['Timestamp'].dt.isocalendar().week.astype(ty)
p['wd'] = p['Timestamp'].dt.dayofweek.astype(ty)
p['q'] = p['Timestamp'].dt.quarter.astype(ty)
# start = ['2022-02-15', '2022-02-15'][0]
# end = ['2022-02-15', '2022-02-15'][0]
# p = p[p['Timestamp']>'2023-04-15']
b = ['Astrid', 'Parc Jean Monnet', 'Les Francs']
# p = p[p['bui'].isin(b)]
# p.index= p['t']
# p = p.groupby(by='bui').resample('1h').mean()
nb = ['Heliport 2 - 3']
p = p[~p['Building'].isin(nb)]

# p = p.groupby(by='Building').sample(frac=1/10,random_state=5)

title = f'<b>Power</b> [kW] vs. <b>Flow</b> [m³/h]'
y = ['Power'][0]
x = ['Flow'][0]
color=['tt', 'Temp', 'Building', 'h', 'm', 'w', 'wd', 'q'][1]
facet_col = ['Building','h', 'm', 'w', 'wd', 'q' ][0]
facet_row=['h', 'm', 'w', 'wd', 'q'][1]
hover_name='Building'


# dfig = p[[x, y, color, facet_col, facet_row, hover_name]].dropna(subset=[x,y])
dfig = p

fig = px.scatter(dfig,
              y=y,
              # y='ave',lø øøfdhdfnkjhgkjjkhkhlll
              x=x,
              # text_auto=True,
              # z= 'aveT',
                # facet_row=facet_row,
                facet_col=facet_col,
                facet_col_wrap=7   ,
                trendline =['ols', 'lowess', 'rolling', 'ewm', 'expanding'][0],
                # nbinsx=20, nbinsy=20, 
                # marginal_x="box",
                # marginal_y="histogram",
                hover_name=hover_name,
                trendline_color_override="black",
                hover_data={'Timestamp'},
                color=color,
                color_continuous_scale= [plotly.colors.cyclical.Twilight, 'bluered'][1],
                title=title
              )

for i in fig.layout.annotations:
    i.text = f"<b>{str.split(i.text,'=')[1]}</b>"
    
fig.update_yaxes(ticksuffix=" kW")
fig.update_xaxes(ticksuffix=" m³/h")

l =[
    [2, 115, 'DT = 50 °C'],
    [2, 93, 'DT = 40 °C'],
    [2.5, 58, 'DT = 20 °C'],
    [2.5, 29, 'DT = 10 °C'],
    ]

axis = []
for i in fig.data:
    if [i.xaxis, i.yaxis]  not in axis:
        axis.append([i.xaxis, i.yaxis])

subplot = fig.data[-1]
for x, y in axis:
    for x1,y1,t in l:
        subplot.x=[0,x1]
        subplot.y=[0,y1]
        subplot.line.color= 'olive'
        subplot.hovertemplate = t
        subplot.xaxis = x
        subplot.yaxis = y
        fig.add_trace(subplot)
        
        
# fig.add_hline(y=0)
# fig.update_traces(showlegend=True)
# fig.update_layout(legend=dict(
#     yanchor="top",
#     y=0.99,
#     xanchor="left",
#     x=1
# ))

# fig.layout.xaxis.range = [-0.01,0.2]
# fig.layout.yaxis.range = [-1,100]

fig.write_html("C:/Users/jborr/OneDrive/Noven/SHW/PVT/PVTfull.html")
fig.show('browser')


#%% ⚠️Flow peaks
result.sort_values(by=['Building','Timestamp'], ascending=[True, True], inplace=True)
result['volume delta'] = result.groupby(by='Building')['Total volume'].diff()
result['heat delta'] = result.groupby(by='Building')['Total heat'].diff()
result['Total days'] = result.groupby('Building')['Timestamp'].max()


#%% ⭐📊graphs 2

import plotly.express as px
import plotly

limSV = result['volume delta'].quantile(0.95)
limSQ = result['heat delta'].quantile(0.95)
limSQ = 150

p = result[
    ((result['volume delta'] >0) & (result['volume delta'] < limSV))
    & ((result['heat delta'] >0) & (result['heat delta'] < limSQ))    
    ]

p = p.reset_index()
nb = ['Heliport 2 - 3']
p = p[~p['Building'].isin(nb)]


fig = px.scatter(p,
              x=['volume delta', 'Volume'][1],
               y=['heat delta', 'Timestamp'][1],
              # x=x,
              # text_auto=True,
              # z= 'aveT',
                # facet_row=facet_row,
                # histnorm = 'percent',
                # barnorm = 'percent',
                facet_col='Building',
                facet_col_wrap=7   ,
                # trendline =['ols', 'lowess', 'rolling', 'ewm', 'expanding'][0],
                # nbinsx=20, nbinsy=20, 
                # marginal_x="box",
                # marginal_y="histogram",
                # hover_name=hover_name,
                # trendline_color_override="black",
                # hover_data={'Timestamp'},
                # color=color,
                # color_continuous_scale= [plotly.colors.cyclical.Twilight, 'bluered'][1],
                # title=title
              )


fig.show('browser')

#%% PGO
import plotly.graph_objects as go

fig = go.Figure(data=go.Scattergl(
    y = p['aveP'],
    x =  p['aveV'],
    mode='markers',
    marker=dict(
        color= p['aveT'],
        colorscale='bluered',
        line_width=1
    )
))
#%% ⭐ Temperature plateau (~15 min)
temps= [30,35,40,45,50,55,60,65,70,75,80,85,90,95]
temps = list(range(30,85))

for i in tqdm(range(len(temps)-1)):
    dfj2[f'above_{temps[i]}'] = np.where(dfj2['param']==parameters[0][0], dfj2['ave'] >= temps[i], np.nan)
    dfj2[f'between_{temps[i]}'] = np.where(dfj2['param']==parameters[0][0], (dfj2['ave'] >= temps[i]) & (dfj2['ave'] < temps[i+1]), np.nan)


# for j in ['5-min', '10-min', 'hour', 'day']:
#     dfjT0 = dfj2[(dfj2['param']==' ' + parameters[1][0]) & (dfj2['variant'] == j)].copy()
dfjT0 = dfj2[(dfj2['param']==parameters[0][0])].copy()
dfjT0.sort_values(by='time', inplace=True)
# b = 'Heliport 2 - 3' 
# i=55
# dfTall = pd.DataFrame()
results = pd.DataFrame()
for i in tqdm(temps):
    for b in tqdm(dfjT0['bui'].unique()):      
        # dfjT = dfjT0[(dfjT0['bui'] == b) & (dfjT0.index.year == 2023) & (dfjT0.index.month >= 0)].resample('1min').first().ffill(limit=60).copy() 
        dfjT = dfjT0[(dfjT0['bui'] == b) & (dfjT0.index.year == 2023) & (dfjT0.index.month >= 0)].copy()
        dfjT2 = dfjT.copy()
        dfjT2['time'] = (dfjT2['time'] + pd.to_timedelta(-1, unit='s')).shift(-1)
        dfjT2.index = pd.to_datetime(dfjT2['time'], format='mixed')
        dfjT2.index = dfjT2.index.tz_localize(None)
        dfjT = pd.concat([dfjT, dfjT2.iloc[:-1,:]])
        dfjT.sort_values(by='time', inplace=True)
        
        dfjT['weekday'] = dfjT.index.weekday
        dfjT['month'] = dfjT.index.month
        dfjT['hour'] = dfjT.index.hour
        dfjT['week'] = dfjT.index.isocalendar().week
        dfjT[f'groupsStart_{i}'] = ((dfjT[f'above_{i}'] != dfjT[f'above_{i}'].shift(1)) & (dfjT[f'above_{i}'] == 1)).cumsum()
        dfjT[f'groupsEnd_{i}'] = ((dfjT[f'above_{i}'] != dfjT[f'above_{i}'].shift(1)) & (dfjT[f'above_{i}'] == 0)).cumsum()
        dfjT[f'groupsDiff_{i}'] = (dfjT[f'groupsStart_{i}'] - dfjT[f'groupsEnd_{i}'])
        dfjT[f'groupsDiff_{i}'] = dfjT[f'groupsDiff_{i}']-dfjT[f'groupsDiff_{i}'].min()
               
        dfjT[f'groups_{i}'] = (dfjT[f'above_{i}'] != dfjT[f'above_{i}'].shift(1)).cumsum()
        # dfTall = dfTall.join(dfjT[['ave', 'min', 'max']], rsuffix=f'_{j}')
        
        
        # dfjT[f'groups_{i}'] = dfjT[f'above_{i}'].cumsum()- dfjT[f'above_{i}'].cumsum().where(~dfjT[f'above_{i}']).ffill().fillna(0).astype(int)        
        grouped = dfjT.groupby([f'groups_{i}','bui'])
        result = grouped.agg(start_time=('time', 'first'),
                         end_time=('time','last'),
                         ave=('ave', 'mean'),
                         maxT=('max', 'mean'),
                         minT=('min', 'mean'),
                         up = (f'groupsDiff_{i}', 'mean'),
                         duration=('time', lambda x: x.iloc[-1] - x.iloc[0]))
        result['duration_mins'] = result['duration'].dt.days*24*60 + result['duration'].dt.seconds/60
        result['range_days'] = (dfjT.index.max().timestamp()-dfjT.index.min().timestamp())/60/60/24
        result['duration_relative'] = result['duration_mins']/(result['range_days']*24*60)
        result['real_end'] = result['start_time'].shift(-1)
        result['real_duration_mins'] = (result['real_end'] - result['start_time']).dt.days*24*60 + (result['real_end'] - result['start_time']).dt.seconds/60
        # result['bui']=result.index.get_level_values(1)
        result['treshold']=f'{i} °C'
        # result['v']=j
        results = pd.concat([results, result])        
        
            
# results['v'] = results.index.get_level_values(2)
results['b'] = results.index.get_level_values(1)
a1 = results[results['up'] == 0].copy()
a2 = results[results['up'] == 1].copy()
a = results.groupby(by=['up','b','treshold'])['duration_relative'].sum()
a = results.groupby(by=['up','b','treshold'])[['duration_mins', 'ave']].mean()
results.to_pickle(r'C:/Users/jborr/OneDrive/Noven/results.pk')
a2.to_csv(r'C:/Users/jborr/OneDrive/Noven/SHW/results.csv')

results = pd.read_pickle(r'C:/Users/jborr/OneDrive/Noven/results.pk')


sela2 = a2[a2['treshold'].isin([ '56 °C', '57 °C',
'58 °C', '59 °C', '60 °C', '61 °C', '62 °C', '63 °C', '64 °C',
'65 °C', '66 °C', '67 °C', '68 °C', '69 °C', '70 °C', '71 °C',
'72 °C', '73 °C'])]
sorted_a2 = sela2.sort_values(by='real_duration_mins', ascending=False).groupby('bui').head(100)

# results = results[(results['duration_mins']>0) & (results['duration_mins']<1*24*60)]

# results2 = results.sort_values('duration_mins',ascending = False).groupby(by=['b','v','treshold']).head(2)
aaa = a2.groupby(by=['bui','treshold'])['real_duration_mins'].describe()
aaa['p1% [min]'] = a2.groupby(by=['bui','treshold'])['real_duration_mins'].quantile(0.01)
aaa['p5% [min]'] = a2.groupby(by=['bui','treshold'])['real_duration_mins'].quantile(0.05)
aaa['p95% [min]'] = a2.groupby(by=['bui','treshold'])['real_duration_mins'].quantile(0.95)
aaa['p99% [min]'] = a2.groupby(by=['bui','treshold'])['real_duration_mins'].quantile(0.99)
aaa['sum [min]'] = a2.groupby(by=['bui','treshold'])['real_duration_mins'].sum()
aaa['sum [min]'] = a2.groupby(by=['bui','treshold'])['real_duration_mins'].sum()

aaa.rename(columns={    
                    '25%': "p25% [min]",
                    '50%': "p50% [min]",
                    'bui': 'Building',
                    '75%': 'p75% [min]',
                    'mean': 'mean [min]',
                    'min': 'min [min]',
                    'max': 'max [min]',
                    'std': 'std [min]'
                    }, inplace=True)
aaa = aaa.round(0)
aaa.reset_index(inplace=True)

aaa['Sum_norm [%]'] = aaa['sum [min]']/aaa.groupby('bui')['sum [min]'].transform('max')
aaa['ave_norm [%]'] = aaa['mean [min]']/aaa.groupby('bui')['mean [min]'].transform('max')

aaa.to_csv(r'C:/Users/jborr/OneDrive/Noven/SHW/summaryResults.csv')
aaa.to_excel(r'C:/Users/jborr/OneDrive/Noven/SHW/summaryResults.xlsx')
#%% Time series by building
# Hauts de Saint Job
# col = dfj2.filter(regex='above').columns
r1 = dfjT0[(dfjT0['bui']=='Parc des Nations')]
# r1 = dfjT0


fig = px.line(r1, 
             # y=['ave','min','max'][0],
              y='ave',
             x = 'time',
              # facet_row=col,        
              color = 'bui',
             )

fig.show('browser')
#%% ⭐ Time series contour
freq = '5min'
limit = 12
dfres = pd.DataFrame()
dfres.index = pd.date_range(freq=freq, start=dfjT0.index.min(), end=dfjT0.index.max())
dfres = dfres.resample(freq).first()
j = 0
for b in tqdm(dfjT0['bui'].unique()):    
    j += 1
    dfjT = dfjT0[dfjT0['bui'] == b].resample(freq).first().ffill(limit=limit).copy() 
    dfres = dfres.join(dfjT[['ave', 'min', 'max']], rsuffix=f'_{j}')


dfplot=pd.DataFrame()
dfp = dfjT0.resample('5min').first()
# mean(numeric_only=True)
dfplot['ave'] = dfres.filter(regex='ave').mean(axis=1).clip(upper=100, lower=10).replace([100,10],[np.nan,np.nan])
dfplot['min'] = dfres.filter(regex='ave').min(axis=1).clip(upper=100, lower=10).replace([100,10],[np.nan,np.nan])
dfplot['minm'] = dfres.filter(regex='min').min(axis=1).clip(upper=100, lower=10).replace([100,10],[np.nan,np.nan])
dfplot['max'] = dfres.filter(regex='ave').max(axis=1).clip(upper=100, lower=10).replace([100,10],[np.nan,np.nan])
dfplot['maxm'] = dfres.filter(regex='max').max(axis=1).clip(upper=100, lower=10).replace([100,10],[np.nan,np.nan])
dfplot['time'] = dfplot.index 
dfplot['bui'] = 'all'

dfplot=dfjT0
# dfplot =dfp
dfPivot = pd.pivot(dfjT0, index='bui',  columns=['ave', 'min', 'max'])

fig = px.line(dfplot,
              y=['ave', 'minm', 'maxm'],
              # y='ave',

              x='time',
              # facet_row=col,
              color='bui',
              )

fig.show('browser')


#%%
# results2 = results[results['up'] == 1]
r = a2[(a2['b']=='Heliport 2 - 3')]
r = a2[(a2['b']=='Hauts de Saint Job')]
r = a2

fig = px.timeline(r, 
                  x_start="start_time", 
                  x_end="end_time", 
                  # facet_row = 'v',
                  color = 'b',
                  y='treshold',
                  )
fig.show('browser')

#%% ⭐ Boxplot

r2 = a2[a2['treshold'] == '60 °C']
r2 = a2[a2['treshold'].isin(a2['treshold'].unique()[35:10:-10])]
# r2 = a2[a2['treshold'].isin(a2['treshold'].unique()[35:5:-5])]
# r2 = a2

# r2['duration_relative2'] = r2['duration_relative']/r2.groupby('bui')['ave'].transform('min')

y = ['duration_relative','duration_relative2', 'duration_mins', 'start_time'][2]


order = r2.groupby(by='b')[y].mean().sort_values().index

fig = px.box(r2, 
             y=y,
             # x = 'b',
                facet_row='treshold', 
                color='b',
              category_orders={'b': order},
               facet_col_wrap=10,

              # hover_data='22'','
              # color = 'up',
             )

# for s in r2['b'].unique():
#     fig.add_annotation(x=s,
#                        y = r2[r2['b']==s][y].max(),
#                        text = str(len(r2[r2['b']==s][y])),
#                        yshift = 10,
#                        showarrow = False,
#                        xref='x'+str(facet.index(i)+1),
#                        yref='y1',                       
#                       )



# overlay bar over boxplot

# upper=r2[y].quantile(0.9)
# fig.update_yaxes(tick0=0, dtick=upper/10, range=[0, upper])
fig.layout.yaxis2.matches = None
fig.layout.yaxis3.matches = None
fig.show('browser')

#%% ⭐⭐ Carpet 

a2['duration_relative2'] = a2['duration_relative']/a2.groupby('bui')['duration_relative'].transform('min')

y = 'duration_relative'
dfplot = a2.groupby(by=['bui','treshold'])[y].sum().reset_index(drop=False)  
dfplot = pd.pivot_table(dfplot, index='treshold', columns='bui', values= y)
dfplot = (dfplot*100).round(1)
dfplot.sort_values(by='treshold', ascending=False, inplace=True)


dfplot = (dfplot*100/dfplot.max()).round(1)
# dfplot['m'] = dfplot.reset_index().index.to_list()
# dfplot['1'] = 2
# dfplot['1']**dfplot['m']

dfpT = dfplot.T
dfpT['sort1'] = dfplot.count()
dfpT['sort2'] = dfplot.sum()
dfpT.sort_values(by=['sort1'], ascending = False, inplace=True)
dfplot = dfpT[dfplot.index].T


# dfplot = dfplot.diff().shift(-1).replace([0.0], np.nan)

fig = px.imshow(dfplot,
                 labels=dict(x="Project", y="Threshold", color="% above treshold"),
                 text_auto=True,
                 aspect="auto",
                 color_continuous_scale='viridis',
                 )

fig.layout.title = "(time above threshold)/(total time)<br><sub>Temperatures below 30 °C have been removed for normalization"

fig.show('browser')

name = 'Carpet'
fig.write_html(fr'C:/Users/jborr/OneDrive/Noven/SHW/{name}.html')
dfplot.to_excel(fr'C:/Users/jborr/OneDrive/Noven/SHW/{name}.xlsx')


#%% ⭐⭐Load duration curve temps
dfl0 = dfj2[dfj2['param']==parameters[0][0]]
# dfl0.resample('1h').min()
dfl2 = pd.DataFrame()


for b in tqdm(dfl0['bui'].unique()):     
    dfl = dfl0[(dfl0['bui'] == b) & (dfl0.index.year == 2023) & (dfl0.index.month > 0)][dfl0.columns[:9]].resample('1h').first().ffill(limit=1).dropna()
    if dfl.shape[0] != 0:
        dfl.sort_values(by='ave', ascending=False, inplace=True)
        
        dfl.reset_index(drop=True, inplace=True)
        dfl['order'] = dfl.index*100/max(dfl.index)
        dfl['mean'] = dfl['ave'].mean()
        dfl['max'] = dfl['ave'].max()
        dfl['min'] = dfl['ave'].min()
        dfl2 = pd.concat([dfl2, dfl])

# dfl = dfl0[(dfl0['bui'] == 'Oxford Picadilly') & (dfl0.index.year == 2023) & (dfl0.index.month == 6)].resample('1h').first().ffill(limit=1)
    
# dfl2['order'] = dfl2.index*100/max(dfl2.index)
dfl2 = dfl2.sort_values(by=['bui', 'order'])

fig = px.line(dfl2, 
             y=['duration_mins', 'ave'][1],
             x = 'order',
             
             # facet_row=1,        
             color = 'bui',
             # color_discrete_sequence=['blue', 'green', 'red'],
             )


fig.layout.title = "Cumulative time above a certain temperature/(total time)<br><sub>All datapoints included - 01/06/2022 to 19/07/2023"
fig.layout.xaxis.title.text = "Percentage of time"
fig.layout.yaxis.title.text = "Temperature [°C]"

fig.show('browser')
name = 'temperature duration curve'
fig.write_html(fr'C:/Users/jborr/OneDrive/Noven/SHW/{name}.html')
dfl2.to_excel(fr'C:/Users/jborr/OneDrive/Noven/SHW/{name}.xlsx')

#%%

import webbrowser
urL='https://www.google.com'
firefox_path="C:/Program Files/Mozilla Firefox/firefox.exe"
webbrowser.register
# webbrowser.register(name='firefox', instance=webbrowser.BackgroundBrowser(firefox_path))

webbrowser.register('firefox', webbrowser.BackgroundBrowser(firefox_path), instance=None, *, preferred=False)
webbrowser.get('firefox')


print(webbrowser._browsers)
#%% hist plotly
results['variant'] = results.index.get_level_values(2)
fig = px.box(results, 
             y=['duration_mins'][0],
             x = 'bui',
              facet_row='variant',        
             color = 'treshold',
             )

fig.show('browser')

# from glob import glob
# d = glob('G:/**/*historische*.xlsx', recursive=True)

elements = {'Alba Regia': '17/90',
            'Bervoets': '17/82',
            'Bordet': '17/74',
            'Broqueville': '17/110',
            'Brunfaut': '17/113',
            'Brusilia': '17/71',
            'Camargue': '17/88',
            'Charles de Lorraine': '17/81',
            'Clos Ceres': '17/92',
            'Eton': '17/97',
            'Expo4': '17/72',
            'Haute les Bois': '17/133',
            'Heliport1': '17/93',
            'Heliport 2-3': '17/91',
            'Les Iris': '17/80',
            'Liberateurs': '17/75',
            'Livingstone': '17/132',
            'Mimosa': '17/77',
            'Parc du Lac': '17/87',
            'Parc Jean Monnet': '17/94',
            'Quattuor': '17/89',
            'RTB': '17/85',
            'St Vincent': '17/76',
            'Up-Site': '17/83',
            'Wiser': '17/104', }

param = {'Export CHP2': ['10059', 'CHP 2'],
         'Import CHP2': ['10058', 'CHP 2'],
         'Thermal Energy CHP2': ['10307', 'CHP 2'],
         'Gas CHP2': ['10453', ''],
         'CHP2 depart': ['10303', 'CHP 2'],
         'CHP2 retour': ['10304', 'CHP 2'],
         'Draaiuren CHP2': ['10077', 'CHP 2'],
         'Export CHP3': ['10059', 'CHP 3'],
         'Import CHP3': ['10058', 'CHP 3'],
         'Thermal Energy CHP3': ['10307', 'CHP 3'],
         'Gas CHP3': ['10454', ''],
         'CHP3 depart': ['10303', 'CHP 3'],
         'CHP3 retour': ['10304', 'CHP 3'],
         'Draaiuren CHP3': ['10077', 'CHP 3'],
         'Export CHP': ['10059', 'CHP 1'],
         'Import CHP': ['10058', 'CHP 1'],
         'Thermal Energy CHP': ['10307', 'CHP 1'],
         'Gas CHP': ['10432', ''],
         'CHP depart': ['10303', 'CHP 1'],
         'CHP retour': ['10304', 'CHP 1'],
         'Draaiuren CHP': ['10077', 'CHP 1'],
         'Draaiuren': ['10077', 'CHP 1'],
         'Export CHP4': ['10059', 'CHP 4'],
         'Import CHP4': ['10058', 'CHP 4'],
         'Thermal Energy CHP4': ['10307', 'CHP 4'],
         'Gas CHP4': ['10455', ''],
         'CHP4 depart': ['10303', 'CHP 4'],
         'CHP4 retour': ['10304', 'CHP 4'],
         'Draaiuren CHP4': ['10077', 'CHP 4'],
         'DM Element': ['DM Element', 'DM Element'], 
         'epoch': ['epoch', 'epoch'], 
         'Time': ['Time', 'Time'],         
         'site': ['site', 'site'],     
         }



files = ['01 Noven UPGRADE/03 Projecten Upgrade/NB_001_BRUSILIA Schaarbeek/0_BRUSILIA Schaarbeek UITVOERING WKK/13_O&M/Historische data Brusilia.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_002_BROQUEVILLE Brussel/0_BROQUEVILLE Brussel UITVOERING/13_O&M/Historische data Broqueville.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_003_BRUNFAUT Molenbeek/Brunfaut [INT]/13_O&M/Historische data Brunfaut.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_004_EXPO4 Jette/0_EXPO 4 Jette UITVOERING WKK/13_O&M/Historische data Expo4.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_006_BORDET Evere/0_BORDET Evere UITVOERING/13_O&M/Historische data Bordet.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_007_LIVINGSTONE Brussel/0_LIVINGSTONE Brussel UITVOERING/13_O&M/Historische data Livingstone.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_008_LIBERATEURS Molenbeek/LIBERATEUR [INT]/13_O&M/Historische data Liberateurs.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_009_ST VINCENT 1 Evere/st vincent I [INT]/13_O&M/Historische data St Vincent.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_010_MIMOSA CHEVREFEUILLE Watermaal-Bosvoorde/mimosa chevrefeuille [INT]/13_O&M/Historische data Mimosa.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_013_LES IRIS Oudergem/les iris_INT/13_O&M/Historische data Les Iris.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_014_CHARLES DE LORRAINE Ukkel/0_CHARLES DE LORRAINE Ukkel UITVOERING WKK/13_O&M/Historische data Charles de Lorraine.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_015_BERVOETS Vorst/bervoets_INT/13_O&M/Historische data Bervoets.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_016_UP-SITE Brussel/0_UP-SITE Brussel UITVOERING/13_O&M/Historische data Up-Site.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_018_RTB II-III Schaarbeek/RTB II-III [INT]/13_O&M/Historische data RTB.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_020_PARC DU LAC Oudergem/parc du lac [INT]/13_O&M/Historische data Parc du Lac.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_021_CAMARGUE Evere/0_CAMARGUE Evere UITVOERING WKK/13_O&M/Historische data Camargue.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_023_QUATTUOR Oudergem/quattuor_INT/13_O&M/Historische data Quattuor.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_024_ALBA REGIA Evere/alba regia_INT/13_O&M/Historische data Alba Regia.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_025_HELIPORT 2-3 Brussel/héliport 2-3_INT/13_O&M/Historische data Heliport 2-3.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_026_CLOS CERES Oudergem/clos ceres_INT/13_O&M/Historische data Clos Ceres.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_027_HELIPORT 1 Brussel/0_HELIPORT 1 Brussel UITVOERING/13_O&M/Historische data Heliport1.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_028_PARC JEAN MONNET Sint-Agatha-Berchem/parc jean monnet_INT/13_O&M/Historische data Parc Jean Monnet.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_029_HAUT LES BOIS Watermaal-Bosvoorde/haut les bois_INT/13_O&M/Historische data Haute les Bois.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_034_WISER 13-16 Etterbeek/wiser 13-16_INT/13_O&M/Historische data Wiser.xlsx',
'01 Noven UPGRADE/03 Projecten Upgrade/NB_035_ETON Ukkel/0_ETON Ukkel - UITVOERING/13_O&M/Historische data Eton.xlsx']

dff=pd.DataFrame()
pre = 'G:/Shared drives/'

for i in tqdm(files):
    dff2 = pd.read_excel(f'{pre}{i}')
    dff2.columns = dff2.columns.str.replace('1','').str.replace('WKK','CHP').str.replace('Retour CHP','CHP retour').str.replace('Depart CHP','CHP depart')
    dff2.dropna(how="all", subset=dff2.columns[1:], inplace=True)
    dff2['site'] = i.split('data ')[1][:-5]
    dff = pd.concat([dff,dff2])
    
dff['DM Element'] = dff['site'].apply(lambda x: elements[x])
dff.index = pd.to_datetime(dff['Time'])
dff.drop(['Time'], axis=1, inplace=True)
dff['epoch'] = dff.index
dff['epoch'] = dff['epoch'].apply(lambda x: x.timestamp())
    
arrays =[
    dff.columns,
    [param[i][0]for i in dff.columns],
    [param[i][1]for i in dff.columns],
    ]
tuples = list(zip(*arrays))
pd.MultiIndex.from_tuples(tuples, names=["original", "DM param", "DM instance"])

dff.to_excel(r'C:/Users/jborr/OneDrive/Noven/historical data.xlsx')

df2 = pd.pivot_table(dff, index=dff.index,columns='site')
df2.sort_index(axis=1,level=[1,0], ascending=True, inplace = True)
df2.to_excel(r'C:/Users/jborr/OneDrive/Noven/historical data wide.xlsx')
dff.to_pickle(r'C:/Users/jborr/OneDrive/Noven/historical data.pk')
df2.to_pickle(r'C:/Users/jborr/OneDrive/Noven/historical data wide.pk')
#%% Import pickle

df  = pd.read_pickle(r'C:\Users\jborr\OneDrive\Noven\calorimeters.pk')
df2 = pd.pivot_table(df, index=df.index,columns=[0,1])
out = df.groupby(by=0).sum().sort_values(by='Trend.AverageValue')[df.groupby(by=0).sum().sort_values(by='Trend.AverageValue')['Trend.AverageValue']<=0].index

df = df[~df[0].isin(out)]
df['hour'] = df.index.hour
df['weekday'] = df.index.weekday
df['month'] = df.index.month
df['quarter'] = df.index.quarter
df['ave'] = df['Trend.AverageValue']
df['max'] = df['Trend.MaximumValue']
df['min'] = df['Trend.MinimumValue']

param = df[1].drop_duplicates().to_list()
p = [
     ' Flow (Calori Meters Heating)',
  ' T Depart (Calori Meters Heating)',
  ' T Retour (Calori Meters Heating)',
  ' Thermal Energy (6h) (Calori Meters Heating)',
 # ' Thermal Power (Calori Meters Heating)',
  # ' Total Thermal Energy (Calori Meters Heating)'
 ]
df2 = df[df[1].isin(p)]
#%%plot

fig = px.box(df2, 
             y=['ave','max','max'][0],
             x = 'hour',
             facet_row=1,        
             # color = 'hour',
             )
fig = fig.update_yaxes(matches=None, showticklabels=True)
fig.show('browser')

# figd=fig.full_figure_for_development()
#%%

fig = px.scatter(df2, 
             y=['ave','max','max'][0],
             x = df2.index,
             facet_col=1,        
             color = 0,
             )
# fig.layout.yaxis.autorange = False
fig = fig.update_yaxes(matches=None, showticklabels=True)
fig.show('browser')
#%%

fig = px.line(df2, 
             y=['ave','max','max'][0],
             x = df2.index,
             facet_col=1,        
             color = 0,
             )
# fig.layout.yaxis.autorange = False
fig = fig.update_yaxes(matches=None, showticklabels=True)
fig.show('browser')


#%%Dtale
import dtale

d = dtale.show(dfj)

#%% divide dfj into 

figd = fig.full_figure_for_development()
