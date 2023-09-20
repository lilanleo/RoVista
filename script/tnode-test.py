import pandas as pd
from datetime import datetime, timedelta
from ripe.atlas.cousteau import (
  Ping,
  Traceroute,
  AtlasSource,
  AtlasCreateRequest,
  AtlasRequest
)

ATLAS_API_KEY = ""
altas_path = ''
altas_result_path = ''
test_asn_path = ''
result_path = ''
data_path = ''
altas_url_path = ''

def get_rov_asn(date):
    data = pd.read_csv(altas_path + f'/{date}/probes.csv')
    data = data['asn'].value_counts().rename_axis('asn').reset_index(name='counts')
    rov = pd.read_csv(test_asn_path + f'/{date}/rov.csv')
    result = pd.merge(data, rov, on='asn')
    result = result[result['ROV']!='not']['asn'].tolist()
    asn_list = []
    for asn in result:
        asn_list.append(int(asn))
    return asn_list

def get_nonROV_asn(date):
    data = pd.read_csv(altas_path + f'/{date}/probes.csv')
    data = data['asn'].value_counts().rename_axis('asn').reset_index(name='counts')
    rov = pd.read_csv(test_asn_path + f'/{date}/non-rov.csv')
    result = pd.merge(data, rov, on='asn')
    result = result[result['ROV']=='not']
    asn_list = []
    for asn in result['asn'].tolist():
            asn_list.append(int(asn))
    return asn_list

def get_target_on_prefix(date):
  
    data = pd.read_csv(f'{result_path}/target/tcp/{date}_combined.csv')[['target']]
    prefix = pd.read_csv(f'{result_path}invalidIPs/tcp/{date}_combined.csv')[['prefix', 'ip']]
    data = pd.merge(data, prefix, right_on='ip', left_on='target')[['target', 'prefix']].drop_duplicates(subset=['prefix'])
    roa = pd.read_csv(f'{data_path}/validation/{date}_combined_invalid.csv')
    data = pd.merge(data, roa, on='prefix')[['target', 'prefix', 'asn', 'origin']]
    data = data.drop_duplicates(subset=['asn', 'origin'])
    print(data)
    return data['target'].tolist()

def get_all_asn(date):
    data = pd.read_csv(altas_path + f'/{date}/probes.csv')
    data = data['asn'].value_counts().rename_axis('asn').reset_index(name='counts')
    data['asn'] = data['asn'].astype(int)
    asns = data['asn'].tolist()
    return asns


def create_job(date):
    targets = get_target_on_prefix(date)
    asns = get_rov_asn(date) + get_nonROV_asn(date)
    measurements = []
    sources = []

    for target in targets:
        measurement = Traceroute(af=4, target=target, description=f"ROV Traceroute on {target}", protocol="ICMP", tags=["rpki-peering"])
        measurements.append(measurement)

    for asn in asns:
        source = AtlasSource(type="asn", value=str(asn), requested=1)
        sources.append(source)
    
    print(f'ASN: {len(sources)}')
    print(f'targets: {len(measurements)}')
    
    atlas_request = AtlasCreateRequest(
        start_time=datetime.utcnow() + timedelta(minutes=1),
        key=ATLAS_API_KEY,
        measurements=measurements,
        sources=sources,
        is_oneoff=True
    )
    (is_success, response) = atlas_request.create()
    print(is_success)
    print(response)

def parse_one_tr(msm):
    dst = msm['dst_addr']
    src = msm['src_addr']
    size = msm['size']
    probe_id = msm['prb_id']
    ip_path = []
    rtt_path = []
    for result in msm['result']:
        try:
            ip_path.append(result['result'][0]['from'])
            rtt_path.append(str(result['result'][0]['rtt']))
        except:
            ip_path.append('*')
            rtt_path.append('*')
    return [dst, src, probe_id, ip_path[-1], rtt_path[-1], ' '.join(ip_path), ' '.join(rtt_path)]


def get_result(id):
    url_path = f'/api/v2/measurements/{id}/results/'
    request = AtlasRequest(**{"url_path": url_path, "key": ATLAS_API_KEY})
    (is_success, results) = request.get()

    if not is_success:
        print("error")
        return 1
    mr = []
    for msm in results:
        mr.append(parse_one_tr(msm))
    results = pd.DataFrame(mr, columns=['dst', 'src', 'probe_id', 'last_hop', 'rtt', 'ip_list', 'rtt_list'])
    return results

def get_msm(date):
    request = AtlasRequest(**{"url_path": altas_url_path, "key": ATLAS_API_KEY})
    (is_success, response) = request.get()
    msm_list = []
    for msm in response['results']:
        tag = msm['tags']
        id = msm['id']
        print(f'{tag}:{id}')
        if f'rov-{date}' in msm['tags']:
            msm_list.append([msm['id'], msm['target']])
    result_list = []
    i = 0
    print(msm_list)
    for result in msm_list:
        print(f'{i}/{len(msm_list)}')
        result_list.append(get_result(result[0]))
        i = i + 1
    results = pd.concat(result_list)
    print(results)
    results.to_csv(f'{result_path}/test_tnode/{date}.csv', index=False)
