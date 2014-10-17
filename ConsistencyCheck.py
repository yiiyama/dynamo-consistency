import os, json, zlib, urllib2, sys
from time import time
import compare, deco
from optparse import OptionParser

def pullAdler(checkString):
    return checkString.split(',')[0].split(':')[1]

BLOCKSIZE=1024*1024*1024

parser = OptionParser()
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--do-checksum',help='Do checksum calculations and comparison.',action='store_true',dest='doCksm')

(opts,args) = parser.parse_args()

mandatories = ['TName']
for m in mandatories:
    if not opts.__dict__[m]:
        print '\nMandatory option is missing\n'
        parser.print_help()
        exit(-1)

TName = opts.TName
skipCksm = not opts.doCksm

startTime = time()

print 'Getting JSON files from PhEDEx if needed...'

if not os.path.exists(TName + '_tfc.json'):
    print 'Getting TFC...'
    os.system('wget --no-check-certificate -O '+TName+'_tfc.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node='+TName)
else:
    print 'Already have TFC...'

tfcFile = open(TName + '_tfc.json')
tfcData = json.load(tfcFile, object_hook = deco._decode_dict)
tfcFile.close()

tfcPath = ''
tfcName = ''

print 'Converting LFN to PFN...'

for check in tfcData['phedex']['storage-mapping']['array']:
    if check['protocol'] == 'direct' and check['element_name'] == 'lfn-to-pfn':
        tfcPath = check['result']
        tfcName = check['path-match']

if tfcPath.split('/')[-2] == tfcName.split('+')[1].split('/')[0]:
    preFix = tfcPath.split('/'+tfcPath.split('/')[-2])[0]
    print 'Looks good...'
else:
    print 'ERROR: Problem with the TFC.'
    exit()

if not os.path.exists(TName + '.json'):
    print 'Getting file list...'
    os.system('wget --no-check-certificate -O '+TName+'.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node='+TName)
else:
    print 'Already have the file list...'

print 'Loading file list. Please wait...'
inFile = open(TName + '.json')
inData = json.load(inFile, object_hook = deco._decode_dict)
inFile.close()

if not os.path.exists(TName + '_phedex.json'):
    print 'Skimming PhEDEx output. Please wait...'
    blockList = []
    for block in inData['phedex']['block']:
        print 'Size of blockList: ' + str(sys.getsizeof(blockList))
        tempBlock=[]
        for repl in block['file']:
            tempBlock.append({'file':preFix + repl['name'],'size':repl['bytes'],'time':repl['time_create'],
                              'adler32':pullAdler(repl['checksum'])})
            blockList.append(tempBlock)

    print 'Writing skimmed file...'
    outParsed = open(TName + '_phedex.json','w')
    outParsed.write(json.dumps(blockList))
    outParsed.close()
    print 'Done with that...'
else:
    print 'Using old skimmed file...'

startDir = tfcPath.split('$1')[0]

if skipCksm:
    print 'Skipping Checksum (Adler32) calculations...'
else:
    print 'Will calculate Checksum unless old file exists...'
if (not skipCksm and not os.path.exists(TName + '_exists.json')) or (skipCksm and not os.path.exists(TName + '_skipCksm_exists.json')):
    print 'Creating JSON file from your directory...'
    print 'Starting walk...'
    existsList = []
    tempBlock=[]
    for subDir in ['mc','data','generator','results','hidata','himc']:
        for term in os.walk(startDir + subDir):
            if len(term[-1]) > 0:
                print term[0]
                tempBlock=[]
                for aFile in term[-1]:
                    print 'Size of existsList: ' + str(sys.getsizeof(existsList))
                    fullName = term[0]+'/'+aFile
                    if not skipCksm:
                        tempFile = open(fullName)
                        asum = 1
                        while True:
                            buffer = tempFile.read(BLOCKSIZE)
                            print 'Buffer size: ' + str(sys.getsizeof(buffer))
                            if not buffer:
                                break
                            asum = zlib.adler32(str(buffer),asum)
                            if asum < 0:
                                asum += 2**32
                        tempFile.close()
                        cksumStr = str(hex(asum))[2:10]
                        print 'Got checksum...'
                    else:
                        cksumStr = 'Not Checked'
                    tempBlock.append({'file':fullName,'size':os.path.getsize(fullName),'time':os.path.getatime(fullName),
                                      'adler32':cksumStr})
                    existsList.append(tempBlock)
                            
    print 'Creating JSON file from directory...'
    if skipCksm:
        outExists = open(TName + '_skipCksm_exists.json','w')
    else:
        outExists = open(TName + '_exists.json','w')
    outExists.write(json.dumps(existsList))
    outExists.close()
    print 'Done with that...'
else:
    print 'Using old directory JSON file...'

print 'Now comparing the two...'
compare.finalCheck(TName,skipCksm)
print 'Elapsed time: ' + str(time() - startTime) + ' seconds'
