#! /usr/bin/python

import os
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-D',help='Directory name of the server location.',action='store',dest='serverDir',metavar='<name>')
(opts,args) = parser.parse_args()

dirs = os.listdir(opts.serverDir)
dirs.sort()

siteList = open(opts.serverDir+'/sitelist.json','w')
siteList.write('{\n')
first = True
for site in dirs:
    if not os.path.isdir(opts.serverDir + '/' + site):
        continue
    if len(os.listdir(opts.serverDir + '/' + site)) == 0:
        continue
    if not first:
        siteList.write(',\n')
    else:
        first = False
    siteList.write('\"' + site + '\":\"' + site + '\"')
siteList.write('\n')
siteList.write('}\n')
