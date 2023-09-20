from scapy.all import sniff, TCP, IP 
import pandas as pd 
from os import listdir
from os.path import isfile, join

pcap_path = ''
spoofing_result_path = ''


def pcap2csv(filename):
    results = []
    packets = sniff(offline=f'{pcap_path}/{filename}')
    for packet in packets:
        flag = packet.getlayer(TCP).flags
        if flag == 'R':
            time = packet[IP].time
            src = packet[IP].src
            id = packet[IP].id
            results.append([time, src, id])

    resultData = pd.DataFrame(results, columns=['time', 'src', 'id'])
    filename = filename[5:-6]
    if filename.endswith('(copy)'):
        filename = filename[:-6]
    filename = filename[:-3]
    resultData.to_csv(f'{spoofing_result_path}/{filename}.csv', index=False)


def parse_pcap():
    filePath = './data/tcpdump3/'
    files = [f for f in listdir(filePath) if isfile(join(filePath, f))]
    count = 0
    for filename in files:
        pcap2csv(filename)
        print(f'{count}/{len(files)}\t{filename}')
        count += 1