import json

inFile = open('T2_US_MIT_skipCksm_missing.txt','r')

dataset = ''
block = ''
for line in inFile:
    if line.startswith('* Dataset: '):
        dataset = line.split(' ')[2].split()[0]
    if line.startswith('* Block  : '):
        block = line.split(' ')[3].split()[0]
        
