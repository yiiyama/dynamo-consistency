import os, json, zlib, urllib2, sys
from time import time
import compare, deco
from optparse import OptionParser

def cleanEmpty():
    for aFile in os.listdir('.'):
        if aFile.startswith(TName):
            if os.stat(aFile)[6] < 10:
                print aFile + ' is (almost) empty, so it is being removed...'
                os.system('rm '+aFile)

def stripFile(fullName):
    fileName = fullName.split('/')[-1]
    return fullName[:len(fullName)-len(fileName)]

def pullAdler(checkString):
    return checkString.split(',')[0].split(':')[1]

BLOCKSIZE=1024*1024*1024

parser = OptionParser()
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--do-checksum',help='Do checksum calculations and comparison.',action='store_true',dest='doCksm')
parser.add_option('--clean',help='Deletes the stray JSON and results files after being stored in tarball.',action='store_true',dest='doClean')
parser.add_option('-N',help='Will do a new download of PhEDEx files.',action='store_true',dest='newPhedex')
parser.add_option('-n',help='Will do a fresh parsing of the PhEDEx file and directory walk.',action='store_true',dest='newParse')

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

print 'Searching for tarball of old files...'

if opts.newPhedex:
    opts.newParse = True
elif os.path.exists(TName + '.tar.gz'):
    print 'Extracting files from tarball...'
    os.system('tar -xvzf ' + TName + '.tar.gz')

print 'Getting JSON files from PhEDEx if needed...'

if not os.path.exists(TName + '_tfc.json'):
    print 'Getting TFC...'
    os.system('wget --no-check-certificate -O '+TName+'_tfc.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node='+TName)
else:
    print 'Already have TFC...'

print 'Checking for empty files...'
cleanEmpty()

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

if not os.path.exists(TName + '_phedex.json') or opts.newParse:
    print 'Loading file list. Please wait...'
    inFile = open(TName + '.json')
    inData = json.load(inFile, object_hook = deco._decode_dict)
    inFile.close()
    print 'Size of inData: ' + str(sys.getsizeof(inData))

    print 'Skimming PhEDEx output. Please wait...'
    blockList = []
    tempBlock = []
    numFilesParsed = 0
    numDumps = 0
    for block in inData['phedex']['block']:
        tempBlock = []
        for repl in block['file']:
            numFilesParsed += 1
            tempBlock.append({'file':repl['name'].split('/')[-1],'size':repl['bytes'],'time':repl['time_create'],
                              'adler32':pullAdler(repl['checksum'])})
        blockList.append({'directory':preFix + stripFile(block['file'][0]['name']),'files':tempBlock})
    del inData
    print 'Writing skimmed file...'
    outParsed = open(TName + '_phedex.json','w')
    outParsed.write(json.dumps(blockList))
    outParsed.close()
    del blockList
    print 'Done with that...'
else:
    print 'Using old skimmed file...'

startDir = tfcPath.split('$1')[0]

if skipCksm:
    print 'Skipping Checksum (Adler32) calculations...'
else:
    print 'Will calculate Checksum unless old file exists...'
if (not skipCksm and not os.path.exists(TName + '_exists.json')) or (skipCksm and not os.path.exists(TName + '_skipCksm_exists.json')) or opts.newParse:
    print 'Creating JSON file from your directory...'
    print 'Starting walk...'
    existsList = []
    tempBlock=[]
    for subDir in ['mc','data','generator','results','hidata','himc','backfill']:
        for term in os.walk(startDir + subDir):
            if len(term[-1]) > 0:
                print term[0]
                tempBlock=[]
                for aFile in term[-1]:
                    fullName = term[0]+'/'+aFile
                    if not skipCksm:
                        tempFile = open(fullName)
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
                    tempBlock.append({'file':aFile,'size':os.path.getsize(fullName),'time':os.path.getatime(fullName),
                                      'adler32':cksumStr})
                existsList.append({'directory':term[0]+'/','files':tempBlock})
    if len(existsList) > 0:
        print 'Creating JSON file from directory...'
        if skipCksm:
            outExists = open(TName + '_skipCksm_exists.json','w')
        else:
            outExists = open(TName + '_exists.json','w')
        outExists.write(json.dumps(existsList))
        outExists.close()
        del existsList
        print 'Done with that...'
    else:
        print 'Exists list is empty...'
else:
    print 'Using old directory JSON file...'

print 'Now comparing the two...'
compare.finalCheck(TName,skipCksm)

print 'Checking for empty files...'
cleanEmpty()

print 'Making tarball for clean storage: ' + TName +'.tar.gz'
os.system('tar -cvzf ' + TName + '.tar.gz ' + TName + '*.json ' + TName + '*results.txt')
print 'Everything stored in: ' + TName +'.tar.gz'

if opts.doClean:
    print 'Cleaning up files... Do not use --clean if you want to leave files out...'
    os.system('rm ' + TName + '*.json ' + TName + '*results.txt')

print 'Elapsed time: ' + str(time() - startTime) + ' seconds'
