import os
from time import sleep
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')

(opts,args) = parser.parse_args()

TName = opts.TName

if os.path.exists('DatasetsInPhedexAtSites.dat'):
    inFile = open('DatasetsInPhedexAtSites.dat','r')
    toDownload = set()
    for line in inFile:
        if line.startswith('/'):
            dataName = line.split('/')[1]
            datasetNameLen = 3
            if len(dataName) >= datasetNameLen:
                dataName = dataName[:datasetNameLen] + "*"
            toDownload.add(dataName)

    numFiles = len(toDownload)
    countFiles = 0
    for dataName in toDownload:
        countFiles += 1
        print(dataName)
        print(' Getting dataset ' + str(countFiles) + ' out of ' + str(numFiles))
        print('')
        print('')
        fileName = TName + "/PhEDEx/" + dataName.strip('*') + ".json"
        if not os.path.exists(fileName):
            os.system('wget --no-check-certificate -O '+fileName+' https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/'+dataName+'/*/*\&node='+TName)
else:
    print "Something is rather wrong."
