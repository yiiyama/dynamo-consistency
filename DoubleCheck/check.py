#! /usr/bin/python

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

for old in os.listdir('PhEDEx'):
    os.remove('PhEDEx/' + old)

for thing in datasetList:
    fileName = 'PhEDEx/'+thing[0].replace('/','__')+'.json'
    if not os.path.exists(fileName):
        os.system('wget -q --no-check-certificate -O '+fileName+' https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset='+thing[0]+'\&node=T2_US_MIT')
