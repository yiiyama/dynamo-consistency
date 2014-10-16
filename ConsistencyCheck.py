import os, json, zlib
import compare
import deco
from optparse import OptionParser

def pullAdler(checkString):
    return checkString.split(',')[0].split(':')[1]

BLOCKSIZE=1024*1024*1024

parser = OptionParser()
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('-s',help='Skips checksum calculations and comparison.',action='store_true',dest='skipCksm')

(opts,args) = parser.parse_args()

mandatories = ['TName']
for m in mandatories:
    if not opts.__dict__[m]:
        print "\nMandatory option is missing\n"
        parser.print_help()
        exit(-1)

TName = opts.TName
skipCksm = opts.skipCksm

tfcFile = open(TName + "_tfc.json")
tfcData = json.load(tfcFile, object_hook = deco._decode_dict)
tfcFile.close()

tfcPath = ''
tfcName = ''

for check in tfcData['phedex']['storage-mapping']['array']:
    if check['protocol'] == 'direct' and check['element_name'] == 'lfn-to-pfn':
        tfcPath = check['result']
        tfcName = check['path-match']

if tfcPath.split('/')[-2] == tfcName.split('+')[1].split('/')[0]:
    preFix = tfcPath.split('/'+tfcPath.split('/')[-2])[0]
else:
    print 'ERROR: Problem with the TFC.'
    exit()

inFile = open(TName + ".json")
inData = json.load(inFile, object_hook = deco._decode_dict)
inFile.close()

if not os.path.exists(TName + '_phedex.json'):
    blockList = []
    for block in inData['phedex']['block']:
        tempBlock=[]
        for repl in block['file']:
            tempBlock.append({'file':preFix + repl['name'],'size':repl['bytes'],'time':repl['time_create'],
                              'adler32':pullAdler(repl['checksum'])})
            blockList.append(tempBlock)

    outParsed = open(TName + '_phedex.json','w')
    outParsed.write(json.dumps(blockList))
    outParsed.close()

startDir = tfcPath.split('$1')[0]

if skipCksm:
    print 'Skipping Checksum (Adler32) calculations!'
if not os.path.exists(TName + '_exists.json'):
    existsList = []
    for subDir in ['mc','data']:
    #for subDir in ['data']:
        for term in os.walk(startDir + subDir):
            if len(term[-1]) > 0:
                print term[0]
                tempBlock=[]
                for aFile in term[-1]:
                    fullName = term[0]+'/'+aFile
                    tempFile = open(fullName)
                    print 'Reading file...'
                    if not skipCksm:
                        asum = 1
                        while True:
                            buffer = tempFile.read(BLOCKSIZE)
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
                            
    if skipCksm:
        outExists = open(TName + '_skipCksm_exists.json','w')
    else:
        outExists = open(TName + '_exists.json','w')
    outExists.write(json.dumps(existsList))
    outExists.close()

compare.finalCheck(TName,skipCksm)
