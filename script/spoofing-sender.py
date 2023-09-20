from multiprocessing import Pool, Manager
from typing import Tuple
from scapy.all import IP, TCP, send
import pandas as pd
import random
from time import sleep
import tqdm

SPICKSIZE = 10
PRESPICKTIME = 10
PROSPICKTIME = 10
INTERVAL = 0.5
THREADCOUNT = 30

# target file listing vVP and tNode pair
#
targetPath = ''


def sendTCPPacket(args):
    targetIP, dport, sport, seqNum, ackNum = args

    ip=IP(dst=targetIP)

    for i in range(0, PRESPICKTIME):
        sleep(INTERVAL)
        SYN=TCP(dport=dport,flags="A", seq=seqNum, ack=ackNum, sport=sport)
        pack = ip/SYN
        send(pack, verbose=False)
        seqNum = seqNum + 3
        ackNum = ackNum + 3

    for i in range(0, SPICKSIZE):
        SYN=TCP(dport=dport,flags="S", seq=seqNum, ack=ackNum, sport=sport)
        pack = ip/SYN
        send(pack, verbose=False)
        seqNum = seqNum + 3
        ackNum = ackNum + 3

    for i in range(0, PROSPICKTIME):
        sleep(INTERVAL)
        SYN=TCP(dport=dport,flags="A", seq=seqNum, ack=ackNum, sport=sport)
        pack = ip/SYN
        send(pack, verbose=False)
        seqNum = seqNum + 3
        ackNum = ackNum + 3


def sendTCP():
    scanData = pd.read_csv(targetPath)
    p = Pool(THREADCOUNT)
    targets  = scanData.values.tolist()
    list(tqdm.tqdm(p.imap(sendTCPPacket, targets), total=len(targets)))
    p.close()

if __name__ == '__main__':
    sendTCP()