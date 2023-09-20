import pandas as pd
import statsmodels.api as sm
import scipy

file_paht = ''
result_path = ''
p, d, q = 1, 0, 0
threshold_single = 0
threshold_double = 0

def check_spick(data):
    pre_spike = data[:10]
    post_spike = data[10:]
    mod = sm.tsa.arima.ARIMA(pre_spike, order=(p, d, q))
    res = mod.fit()
    predicts = res.forecast(step=len(post_spike))
    a = []
    for i in range(0, len(post_spike)):
        a.append(post_spike[i] - predicts[i])

    zscores = scipy.stats.zscore(a, axis=0, ddof=0, nan_policy='propagate')
    spike = []
    spike_count = 0
    for i in range(0, len(zscores)):
        if zscores[i] > threshold_double:
            spike.append([i, 2])
            spike_count = spike_count + 2
        elif zscores[i] > threshold_single:
            spike.append([i, 1])
            spike_count = spike_count + 1

    return [spike, spike_count]

def check_type(result):
    spike, spike_count = result
    if spike_count > 3:
        spike_type = 'misclass'
    elif spike_count == 1:
        spike_type = 'inbound'
    elif spike_count > 1:
        spike_type = 'outbound'
    else:
        spike_type = 'no'
    return spike_type

def spike2string(result):
    spike, spike_count = result
    results = ''
    for i in range(0, len(spike)):
        results = results + str(spike[i][0]) + ':' + str(spike[i][1]) + ':' + str(spike_count)
    return results


def parse_ipid(data):
    # time, asn, ip, port, 
    data['ids'] = data['ids'].apply(lambda x: x.split(' '))
    data['spike'] = data['ids'].apply(check_spick)
    data['type'] = data['spike'].apply(check_type)
    data['spike'] = data['spike'].apply(spike2string)
    data = data.to_csv(result_path, index=False)
    return data