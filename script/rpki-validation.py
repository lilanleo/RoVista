import pandas as pd 
import ipaddress
import numpy as np
import os

dataPath = ''
resultPath = ''

def getVRPPrefixList(prefix):
    net = ipaddress.ip_network(prefix)
    length = prefix.split('/')[-1]
    length = int(length)
    prefixList = [net]
    while length < 24:
        length = length+1
        prefixList.extend(list(net.subnets(new_prefix=length)))
    prefixList = [prefix.with_prefixlen for prefix in prefixList]
    return prefixList

def expandVRP(line):
    prefix = line['IP Prefix']
    origin = line['ASN'][2:]
    maxLen = line['Max Length']
    prefixLen = prefix.split('/')[-1]
    if ':' in prefix:
        return [[0, 0, 0, 0, 0]]
    try:
        prefixList = getVRPPrefixList(prefix)
        resultList = [[ip, origin, prefix, prefixLen, maxLen] for ip in prefixList]
    except:
        return [[0, 0, 0, 0, 0]]
    return resultList

    
# ip, origin, prefixLen, maxLen
def getVRP(date):
    if os.path.exists(f'{dataPath}/vrp/{date}.csv'):
        print('vrp exit')
        return 0
    print('vrp not exit, processing vrp')
    fileName = 'vrps-' + date[:4] + '-' + date[4:6] + '-' + date[6:]
    filePath = f'{dataPath}/vrps/vrps/{fileName}.csv'
    VRPData = pd.read_csv(filePath, header=0)
    VRPData = VRPData.apply(expandVRP, axis=1).explode()
    VRPData = pd.DataFrame(VRPData, columns=['result'])
    VRPData['ip'] = VRPData['result'].apply(lambda x: x[0])
    VRPData['origin'] = VRPData['result'].apply(lambda x: x[1])
    VRPData['vrpPrefix'] = VRPData['result'].apply(lambda x: x[2])
    VRPData['maxLen'] = VRPData['result'].apply(lambda x: x[4])
    VRPData = VRPData[VRPData['ip']!=0]
    VRPData = VRPData.drop('result', axis=1)
    VRPData.to_csv(f'{dataPath}vrp/{date}.csv', index=False)
    print('vrp processed')
    return VRPData

# ip asn prefix bgpPrefixLen
def getBGP(date):
    if os.path.exists(f'{dataPath}bgp/{date}.csv'):
        print('bgp data exit')
        return 0
    print('bgp data not exit, processing bgp')
    filePath = f'{dataPath}routeview/{date}.csv'
    BGPData = pd.read_csv(filePath, sep='|', header=None, names=['TABLE_DUMP2', 'date', 'type', 'fromIP', 'fromASN', 'prefix', 'ASPath', 'IGP'], index_col=False)
    BGPData = BGPData[(BGPData['type']=='B')&(BGPData['IGP']=='IGP')]
    BGPData['asn'] = BGPData['ASPath'].apply(lambda x: x.split(' ')[-1])
    BGPData = BGPData.drop(['TABLE_DUMP2', 'date', 'type', 'ASPath', 'IGP', 'fromIP', 'fromASN'], axis=1)
    BGPData.to_csv(f'{dataPath}bgp/{date}.csv', index=False)
    print('bgp processed')
    return BGPData     

def validate(line):
    if np.nan in line:
        return 0
    try:
        origin = int(line['origin'])
        asn = int(line['asn'])
        #vrpPrefix = line['vrpPrefix']
        BGPPrefixLen = int(line['prefix'].split('/')[-1])
        maxLen = int(line['maxLen'])
    except:
        return 0
    
    if asn != origin:
        return 1
    else:
        if BGPPrefixLen > maxLen:
            return 2
        return 3


def validateBGP(bgpdate, vrpdate):
    if os.path.exists(f'{dataPath}validation/{bgpdate}_combined.csv'):
        print('validation data exit')
        return 0
    bgpData = pd.read_csv(f'/home/weitong/rpki/result/bgptables/{bgpdate}_origin.csv')
    vrpData = pd.read_csv(f'{dataPath}vrp/{vrpdate}.csv')
    vrpData = vrpData.rename(columns={'ip':'prefix'})
    data = pd.merge(bgpData, vrpData, on=['prefix'], how='left').dropna()
    data['validation'] = data.apply(validate, axis=1)
    theMax = data.groupby(['prefix', 'asn'])['validation'].transform(max) == data['validation']
    data = data[theMax]
    data = data.dropna().drop_duplicates()
    data.to_csv(f'{dataPath}validation/{bgpdate}_combined.csv', index=False)
    print(data['validation'].value_counts())
    invalidData = data[data['validation']==1]
    invalidData.to_csv(f'{dataPath}validation/{bgpdate}_combined_invalid.csv', index=False)
    data = data.groupby('prefix')['validation'].max().reset_index()
    data = data[data['validation']==1]
    data.to_csv(f'{dataPath}validation/{bgpdate}_combined_invalid_unique.csv', index=False)
    print('validation result saved')
    return 0


def validateBGPfromRouteview(date, vrpDate):
    print(f'validateBGPfromRouteview {date}')
    getVRP(vrpDate)
    getBGP(date)
    validateBGP(date, vrpDate)
