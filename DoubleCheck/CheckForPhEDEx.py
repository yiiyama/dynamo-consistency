#! /usr/bin/python

import os

directoryContents = sys.argv[1].split('/')

storeIndex = directoryContents.index('store')
dataset = directoryContents[storeIndex + 2]

if not os.path.exists(os.environ['ConsistencyCacheDirectory'] + '/' +
                      os.environ['site'] + '/PhEDEx/' + dataset + '.json'):
    addData = open('addDat.txt','w')
    addData.write(dataset + '\n')
    exit(1)

exit(0)
