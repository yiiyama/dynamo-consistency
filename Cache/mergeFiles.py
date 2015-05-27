import deco, LFN2PFNConverter
import tarfile, os, json, sys
from optparse import OptionParser

def cleanEmpty():
    for aFile in os.listdir('.'):
        if aFile.startswith(TName):
            if os.stat(aFile)[6] < 10:
                print aFile + ' is (almost) empty, so it is being removed...'
                os.system('rm '+aFile)
                return True
    return False

parser = OptionParser()

parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_lfn2pfn.json.',
                  dest='TName',action='store',metavar='<name>')

(opts,args) = parser.parse_args()

if not opts.__dict__['TName']:
    print ''
    parser.print_help()                                         # Otherwise exit the program
    print ''
    print '******************************************************************'
    print ''
    print 'Give a site name with -T'
    print ''
    print '******************************************************************'
    exit(-1)

TName = opts.TName                                              # Name of the site is stored here

# First load the list of files that were deleted. Make sure they are not added to the final list.
try:
    inFile = open(TName + '/' + TName + '_formatted_deleted.json')
    inData = json.load(inFile, object_hook = deco._decode_dict)
    inFile.close()
except:
    print 'File list wasn\'t successfully loaded.'
    print 'Exiting...'
    exit()

deletedList = []

for block in inData['block']:
    deletedList.append([block['name'],block['time']])

# Next move on to the files that we should search for. Ignore deleted blocks and duplicates.
if not os.path.exists(TName+'/'+TName+'_lfn2pfn.json'):
    print 'Getting TFC...'
    os.system('wget --no-check-certificate -O '+TName+'/'+TName+'_lfn2pfn.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?node='+TName+'\&protocol=direct\&lfn=/store/data/test.root')

prefix = LFN2PFNConverter.GetPrefix(TName)                       # Get the file prefix using the TFC file

try:
    inFile = open(TName + '/' + TName + '_formatted_added.json')
    inData = json.load(inFile, object_hook = deco._decode_dict)  # This step takes a while
    inFile.close()
except:
    print 'File list wasn\'t successfully loaded.'
    print 'Exiting...'
    exit()

duplicateList = []

blockList = []
for block in inData['block']:
    found = False
    for deleted in deletedList:
        if str(block['dataset']) == str(deleted[0]):
            found = True
            break
    if found:
        continue
    for duplicate in duplicateList:
        if str(block['dataset']) == str(duplicate):
            found = True
            break
    if found:
        continue
    duplicateList.append(block['dataset'])
    blockList.append({'directory':prefix+"/"+str(block['directory'])+"/",'files':block['files'],'dataset':str(block['dataset'])})

outfile = open(TName+'/'+TName+'_phedex.json','w')
outfile.write(json.dumps(blockList))
outfile.close()
