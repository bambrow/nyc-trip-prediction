import pandas as pd
import geocoder

df = pd.read_csv('taxi-zone-lookup.csv',header='infer')


def gen_cor(row):
    s = row['Borough'] + ' ' + row['Zone']
    g = geocoder.google(s)
    return g.lat, g.lng

df['Lat'], df['Lon'] = zip(*df.apply(lambda row: gen_cor(row),axis=1))
df.set_index('LocationID',inplace=True)
df.to_csv('taxi-zone-coordinates.csv')
