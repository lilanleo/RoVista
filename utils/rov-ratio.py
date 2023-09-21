import pandas as pd 
import os

dataPath = ''

def rov_overtime(resultPath):
    dates = os.listdir(f'{dataPath}/rov/')
    results = []
    for date in dates:
        data = pd.read_csv(f'{dataPath}/{date}/')
        data = data[data['count']>='10'][['asn', 'target_ip', 'result']]
        data = data.groupby(['asn', 'target_ip']).mean().reset_index().rename(columns={'result':'mean'})
        data = data[(data['mean']==1)|(data['mean']==0)]
        data = data.groupby('asn').mean().reset_index().rename(columns={'result':'filter_rate'})
        ratio = data[data['filter_rate']==1].shape[0]/data.shape[0]
        time = date[:10]
        results.append([time, ratio])
    
    result = pd.DataFrame(results, columns=['date', 'ratio'])
    print(result)
    result.to_csv(f'{resultPath}/rov-overtime-update.csv', index=False)
