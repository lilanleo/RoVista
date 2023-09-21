import requests
from multiprocessing import *
import json
import os
import re
from multiprocessing import Pool, Manager

import pandas as pd 

mrt2bgpdumpPath = ''
dataPath = ''
resultPath = ''

def download_one_probe(target):
    date, probe = target
    print(f'try download rib data of {date} on {probe}')
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    time = date[8:]
    if probe == 'bgpdata':
        url = f'http://archive.routeviews.org/{probe}/{year}.{month}/RIBS/rib.{year}{month}{day}.{time}.bz2'
    else:
        url = f'http://archive.routeviews.org/{probe}/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{time}.bz2'
    path = dataPath + f'/{probe}/' + url.split('/')[-1]
    cmd = f'wget {url} -O {path}'
    if os.path.exists(f'{dataPath}/{probe}/rib.{year}{month}{day}.{time}.bz2'):
        print('bz2 file exit')
        os.system(cmd)
    else:
        print(cmd)
        os.system(cmd)
    if os.path.exists(f'{dataPath}/{probe}/{date}.csv'):
        print('bgpdump file exit')
        os.system(cmd)
    else:
        cmd = f'python {mrt2bgpdumpPath} -O {dataPath}/{probe}/{date}.csv {path}'
        print(cmd)
        os.system(cmd)
    print(f'{probe} done')

def get_probes():
    probes = os.listdir(f'{dataPath}/bgpdump-parsed-reduced')
    probe_list = []
    for probe in probes:
        if probe[-1] != '2':
            probe_list.append(probe)
    return probe_list

def delete_table(date):
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    time = date[8:]
    probes = get_probes()
    for probe in probes:
        cmd = f'rm {dataPath}/{probe}/rib.{year}{month}{day}.{time}.bz2'
        print(cmd)
        try:
            os.system(cmd)
        except:
            continue

def get_asn(path):
    try:
        return path.split(' ')[-1]
    except:
        return 0

def combine_table(date):
    probes = get_probes()
    results = []
    for probe in probes:
        print(probe)
        BGPData = pd.read_csv(f'{dataPath}/{probe}/{date}.csv', sep='|', header=None, names=['TABLE_DUMP2', 'date', 'type', 'fromIP', 'fromASN', 'prefix', 'ASPath', 'IGP'], index_col=False, error_bad_lines=False)
        BGPData = BGPData[(BGPData['type']=='B')&(BGPData['IGP']=='IGP')]
        BGPData['asn'] = BGPData['ASPath'].apply(get_asn)
        BGPData = BGPData.drop(['TABLE_DUMP2', 'date', 'type', 'IGP', 'fromIP', 'fromASN'], axis=1)
        BGPData = BGPData.drop_duplicates()
        print(BGPData)
        results.append(BGPData)
    data = pd.concat(results)
    data = data.drop_duplicates()
    data.to_csv(f'{resultPath}/bgptables/{date}.csv', index=False)


def download_ribs(date):
    probes = get_probes()
    target_list = [[date, probe] for probe in probes]
    p = Pool(5)
    p.map(download_one_probe, target_list)

def get_bgp_table(date):
    download_ribs(date)
    combine_table(date)
    delete_table(date)
