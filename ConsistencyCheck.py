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

# Here we have the list of options. Most importantly, we want to define the name of the site.
# This will allow the code to get the correct information from PhEDEx as well as name the output
# nicely for later storage. In addition, there are options for checksums, cleaning up after itself
# and various levels of redoing sections. (Honestly though, those last options were mostly only
# useful for speeding up the debugging process.)
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--do-checksum',help='Do checksum calculations and comparison.',action='store_true',dest='doCksm')
parser.add_option('--clean',help='Deletes the stray JSON and results files after being stored in tarball.',action='store_true',dest='doClean')
parser.add_option('-N',help='Will do a new download of PhEDEx files.',action='store_true',dest='newDownload')
parser.add_option('-n',help='Will do a fresh parsing of the PhEDEx file and directory walk.',action='store_true',dest='newWalk')
parser.add_option('-p',help='Will only do a fresh parse of the PhEDEx file.',action='store_true',dest='newPhedex')

(opts,args) = parser.parse_args()

mandatories = ['TName']
for m in mandatories:                  # If the name of the site is not specified, end the program.
    if not opts.__dict__[m]:
        print '\nMandatory option is missing\n'
        parser.print_help()
        exit(-1)

TName = opts.TName                     # Name of the site is stored here
skipCksm = not opts.doCksm             # Skipping checksums became the default

startTime = time()                     # Start timing for a final readout of the run time

print 'Searching for tarball of old files...'

if opts.newDownload:                   # If your starting fresh enough for a new download, walk the PhEDEx file and directory again
    opts.newWalk = True
elif os.path.exists(TName + '.tar.gz'):                             # If you're not downloading again, pull files from the tarball
    print 'Extracting files from tarball...'
    os.system('tar -xvzf ' + TName + '.tar.gz')
if opts.newWalk:                       # If walking the directory, parse the PhEDEx JSON file again
    opts.newPhedex = True              # (Again, this is probably used for debugging reasons and some format was changed)

print 'Checking for empty files...'    # If anything from the tarball is empty or almost empty, remove it so that things are rerun
cleanEmpty()

if not os.path.exists(TName + '_tfc.json') or opts.newDownload:     # Download TFC if needed or asked for
    print 'Getting JSON files from PhEDEx...'
    print 'Getting TFC...'
    os.system('wget --no-check-certificate -O '+TName+'_tfc.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node='+TName)
else:
    print 'Already have TFC...'

tfcFile = open(TName + '_tfc.json')
tfcData = json.load(tfcFile, object_hook = deco._decode_dict)       # This converts the unicode to ASCII strings (see deco.py)
tfcFile.close()

tfcPath = ''
tfcName = ''

print 'Converting LFN to PFN...'

for check in tfcData['phedex']['storage-mapping']['array']:         # This is basically just checking that the TFC has an entry I understand
    if check['protocol'] == 'direct' and check['element_name'] == 'lfn-to-pfn' and check['path-match'].endswith('+store/(.*)'):
        tfcPath = check['result']
        tfcName = check['path-match']

if tfcPath.split('/')[-2] == tfcName.split('+')[1].split('/')[0]:   # If the format matches, it'll have a /store/$1 at the end
    preFix = tfcPath.split('/'+tfcPath.split('/')[-2])[0]           # which I can just take off and add to the front of the LFN
    print 'Looks good...'
else:
    print 'ERROR: Problem with the TFC.'                            # If the format is unexpected, I give up
    exit()

if not os.path.exists(TName + '.json') or opts.newDownload:         # Download JSON file list from PhEDEx if needed or asked for
    print 'Getting file list...'
    os.system('wget --no-check-certificate -O '+TName+'.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node='+TName)
else:
    print 'Already have the file list...'

if not os.path.exists(TName + '_phedex.json') or opts.newPhedex:    # Parse the JSON file if need to asked for to make a new format
    print 'Loading file list. Please wait...'
    inFile = open(TName + '.json')
    inData = json.load(inFile, object_hook = deco._decode_dict)     # This step takes a while
    inFile.close()
    print 'Size of inData: ' + str(sys.getsizeof(inData))

    print 'Skimming PhEDEx output. Please wait...'
    blockList = []
    tempBlock = []
    lastDirectory = ''                 # Okay, so I want to keep track of every time there's a directory change and make
    lastBlock = ''                     # a new list entry of the directory. (There are some duplicate directories, but that's fine)
    for block in inData['phedex']['block']:
        for repl in block['file']:
            if preFix + stripFile(repl['name']) != lastDirectory:   # This is where I spot the directory change
                if len(lastDirectory) > 0:                          # If it's not the first directory, I append the old directory info and reset
                    blockList.append({'dataset':lastBlock,'directory':lastDirectory,'files':tempBlock})  # Information for each directory
                    tempBlock = []
                lastBlock = block['name']                           # After adding, I update the block
                lastDirectory = preFix + stripFile(repl['name'])    # and directory information
            for getTime in repl['replica']:                         # Getting creation time of the replica. Give the way our request is
                if getTime['node'] == TName:                        # done, this step might be unnecessary, but it doesn't take too long
                    tempTime = getTime['time_create']
            tempBlock.append({'file':repl['name'].split('/')[-1],'size':repl['bytes'],'time':tempTime,   # Information for each file
                              'adler32':pullAdler(repl['checksum'])})                                    # is stored here
    blockList.append({'dataset':block['name'],'directory':preFix + stripFile(repl['name']),'files':tempBlock})  # Don't forget the last directory
    del inData                         # This is an attempt to free memory. I'm not convinced it's working...
    print 'Writing skimmed file...'
    outParsed = open(TName + '_phedex.json','w')
    outParsed.write(json.dumps(blockList))
    outParsed.close()
    del blockList                      # This is an attempt to free memory. I'm not convinced it's working...
    print 'Done with that...'
else:                                  # If not parsing the JSON file, let the user know
    print 'Using old skimmed file...'

startDir = tfcPath.split('$1')[0]      # This is will be the starting location of the walk through the site's existing files

if skipCksm:
    print 'Skipping Checksum (Adler32) calculations...'
else:
    print 'Will calculate Checksum unless old file exists...'
if (not skipCksm and not os.path.exists(TName + '_exists.json')) or (skipCksm and not os.path.exists(TName + '_skipCksm_exists.json')) or opts.newWalk:
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
