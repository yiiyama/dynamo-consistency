import os
from time import sleep
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')

(opts,args) = parser.parse_args()

TName = opts.TName

if os.path.exists(TName + '/datasetList.txt'):
    inFile = open(TName + '/datasetList.txt','r')
    for line in inFile:
        if line.startswith('  "'):
            dataName = line.split('"')[1] + "_"
            print dataName
            sleep(0.5)
            fileName = TName + "/PhEDEx/" + dataName + ".json"
            os.system('wget --no-check-certificate -O '+fileName+' https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/'+dataName+'*/*/*\&node='+TName)
else:
    print "Something is rather wrong."

