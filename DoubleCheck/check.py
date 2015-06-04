import json, os

inFile = open('T2_US_MIT_skipCksm_missing.txt','r')

dataset = ''
block = ''
first = True
datasetList = []
fileList=[]
for line in inFile:
    if line.startswith('* Dataset: '):
        if not first:
            datasetList.append([dataset,block,fileList])
            fileList=[]
        else:
            first = False
        dataset = line.split(' ')[2].split()[0]
    if line.startswith('* Block  : '):
        block = line.split(' ')[4].split()[0]
    if line.startswith('/'):
        fileList.append(line.split()[0])
        if os.path.exists(line.split()[0]):
            print "File Exists: " + line.split()[0]

