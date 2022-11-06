import os
import datetime
from collections import defaultdict

data = defaultdict(dict)
path = f'S:\\newl\\ib_data_gather\\dumps\\exchanges\\IDEALPRO\\data\\tickers\\5m'
for mode in ['ASK', 'BID']:
    for ticker in os.listdir(f'{path}\\{mode}'):
        timestamps = sorted([datetime.datetime.strptime(i.split('.')[0],"%Y%m%d_%H%M%S") for i in os.listdir(f'{path}/{mode}/{ticker}')], reverse=True)
        deltas = [(timestamps[i] - timestamps[i+1], (timestamps[i+1], timestamps[i]))  for i in range(len(timestamps)-1)]
        for delta, ts in deltas:
            data[(ticker, mode)][delta] = data[(ticker, mode)].get(delta, []) + [ts]

for k,v in data.items():
    print(k)
    for k2,v2 in v.items():
        if len(v2)<10:
            print(k2, v2)
        else:
            print(k2, len(v2))

