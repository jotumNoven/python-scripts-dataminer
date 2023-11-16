import pandas as pd
from glob import glob

f = glob(r'C:/Users/jborr/OneDrive/Noven/Repos/python-scripts-dataminer/ZeroFriction/VME Waterfront Parkhotel SHW/files/*.csv')

df = pd.concat([pd.read_csv(file, delimiter=';', decimal=',') for file in f], ignore_index=True)

#drop last column
df = df.iloc[:, :-1]

#set  column Created as index, convert it to datetime, and sort indexm
df = df.set_index('Created')
df.index = pd.to_datetime(df.index)
df = df.sort_index()
df.columns = ['A','B']
df['C'] = df['A'] - df['B']
# resample, interpolate and plot
df = df.resample('h').mean().interpolate()

df['diffC'] = df['C'].diff()

#plot a barplot of the differencem using plotly express instead of matplotlib, in pandas
pd.options.plotting.backend = ["matplotlib","plotly"][1]

df['diffC'].plot(kind='bar').show('browser')

df['Serial number'] = 'VME Waterfront Parkhotel SHW'
df['Type'] =  'hottapwater'
df['Unit'] = 'm3'
df['Value'] = df['C'].cummax()
df['Datum local time'] = df.index.tz_localize('UTC').tz_convert('Europe/Brussels').strftime('%d/%m/%Y %H:%M')
df['Datum UTC'] = df.index.tz_localize('UTC').strftime('%d/%m/%Y %H:%M')

out = df
out = out[['Serial number', 'Datum UTC', 'Datum local time', 'Value', 'Unit',  'Type']].copy()
out['Value'] = out['Value'].round(2)
out.to_csv("C:/Users/jborr/OneDrive/Escritorio/VDAB/ZF - VME Waterfront Parkhotel SHW.csv", sep=";", decimal=",", index=False)

fig = px.line(df, 
                y=['C', 'Value'],
                # y=['VME', 'diffVME', 'diffSolar', 'diffInj', 'diffY_DM'],
              # x = 'hour',
              # y = ['B', 'dA'],
             )

fig.show('browser')
fig.write_html("C:/Users/jborr/OneDrive/Escritorio/VDAB/ZF - VME Waterfront Parkhotel SHW.html")

