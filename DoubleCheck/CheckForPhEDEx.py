#! /usr/bin/python

import os, sys

directoryContents = sys.argv[1].split('/')

storeIndex = directoryContents.index('store')
dataset = directoryContents[storeIndex + 3]

if not os.path.exists(os.environ['ConsistencyCacheDirectory'] + '/' +
                      os.environ['site'] + '/PhEDEx/' + dataset + '.json'):
    print ('Adding ' + dataset)
    addData = open(os.environ['fileBase'] + '_addData.txt','a')
    addData.write(dataset + '\n')
    addData.close()
    exit(1)

exit(0)
