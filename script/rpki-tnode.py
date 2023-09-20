import ipaddress
import pandas as pd

scanning_file = ''
validation_file = ''
target_path = ''
ONLY_FIRST = True

def maskIP(address):
    return str(address).rsplit('.', 1)[0]

def get_supernet_list(prefix):
    net = ipaddress.ip_network(prefix)
    length = prefix.split('/')[-1]
    length = int(length)
    prefixList = [net]
    while length >=8:
        length = length - 1
        prefixList.append(net.supernet(new_prefix=length))
    prefixList = [prefix.with_prefixlen for prefix in prefixList]
    return prefixList

def get_subnet_addr(prefix):
    net = ipaddress.ip_network(prefix)
    length = prefix.split('/')[-1]
    length = int(length)
    prefixList = [net]
    while length < 24:
        length = length+1
        prefixList.extend(list(net.subnets(new_prefix=length)))
    prefixList = [prefix.with_prefixlen for prefix in prefixList]
    prefixList = ['.'.join(prefix.split('.')[:-1]) for prefix in prefixList]
    prefixList = list(set(prefixList))
    return prefixList


def load_target_file():
    tcp_data = pd.read_csv(scanning_file)
    tcp_data['addr'] = tcp_data['saddr'].apply(maskIP)
    tcp_data = tcp_data.drop(['timestamp_ts', 'sport', 'daddr', 'dport', 'ipid', 'ttl'], axis=1)
    return tcp_data

def load_data(date):
    validated = pd.read_csv(validation_file) 
    valid = validated[validated['validation']==3]
    invalid = validated[validated['validation']==1]
    invalid['covers'] = invalid['prefix'].apply(get_supernet_list)
    invalid = invalid.explode('covers').rename(columns={'prefix':'invalid'})
    valid['has_valid'] = 1
    data = pd.merge(invalid, valid, left_on='covers', right_on='prefix', how='left').fillna(0)
    data = data.sort_values(by=['invalid', 'has_valid'], ascending=False).drop_duplicates(subset=['invalid'], keep='first')
    data = data[data['has_valid']==0][['invalid', 'asn_x']].rename(columns={'invalid':'prefix'})
    data['addr'] = data['prefix'].apply(get_subnet_addr)
    data = data[['prefix', 'addr', 'asn_x']].rename(columns={'asn_x':'origin'})
    data = data.explode('addr')
    # explode invalid prefix to less specific prefixd
    print('loading tcp scanning files')
    tcp_data = load_target_file()
    data = pd.merge(data, tcp_data, on='addr')
    data = data[['prefix', 'saddr', 'origin']]
    return data

def update_targets(date):
    data = load_data(date)
    if ONLY_FIRST:
        data = data.drop_duplicates(subset='prefix', keep='first')
    count = data.shape[0]
    day = date[:4] + '-' + date[4:6] + '-' + date[6:]
    data.to_csv(target_path + f'{day}-all.csv', index=False)
