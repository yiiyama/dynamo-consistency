import os, json, zlib, urllib2, sys
from time import time
import compare, deco
from optparse import OptionParser
import ConfigParser

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
parser.add_option('-c',help='Names a configuration file. If configuration file is present, all other options are ignored.',
                  dest='configName',action='store',metavar='<name>')
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--do-checksum',help='Do checksum calculations and comparison.',action='store_true',dest='doCksm')
parser.add_option('-N',help='Will do a new download of PhEDEx files.',action='store_true',dest='newDownload')
parser.add_option('-n',help='Will do a fresh parsing of the PhEDEx file and directory walk.',action='store_true',dest='newPhedexAndWalk')
parser.add_option('-p',help='Will only do a fresh parse of the PhEDEx file.',action='store_true',dest='newPhedex')
parser.add_option('-d',help='Will only do a fresh directory walk.',action='store_true',dest='newWalk')

(opts,args) = parser.parse_args()

if not opts.__dict__['configName']:                                 # User must specify a configuration file
    subDirs = ['mc','data','generator','results','hidata','himc']
    if not opts.__dict__['TName']:                                  # or give a site name
        print ''
        parser.print_help()                                         # Otherwise exit the program
        print ''
        print '******************************************************************'
        print ''
        print 'You did not give a site name or specify a configuration file!'
        print 'Give a site name with -T or configuration file with -c'
        print ''
        print '******************************************************************'
        exit(-1)
else:                                                               # If a configuration file is given
    config = ConfigParser.RawConfigParser()                         # Overwrite or set all other options
    config.read(opts.configName)
    opts.TName             = config.get('General','SiteName')
    subDirs                = (config.get('ConsistencyCheck','Directories')).strip(' ').split(',')
    opts.doCksm            = config.getboolean('ConsistencyCheck','doChecksum')
    opts.newDownload       = config.getboolean('ConsistencyCheck','DownloadPhEDEx')
    opts.newPhedexAndWalk  = config.getboolean('UseCache','ParsePhEDExAndDir')
    opts.newPhedex         = config.getboolean('UseCache','ParsePhEDEx')
    opts.newWalk           = config.getboolean('UseCache','ParseDir')

TName = opts.TName                                                  # Name of the site is stored here
skipCksm = not opts.doCksm                                          # Skipping checksums became the default

startTime = time()                                                  # Start timing for a final readout of the run time
oldTime = 604800                                                    # If the PhEDEx file hasn't been downloaded for a week, redownload everything

isOld = False
if os.path.exists(TName + '.tar.gz'):
    if startTime - os.path.getctime(TName + '.tar.gz') > oldTime:
        isOld = True
if os.path.exists(TName + '.json'):
    if startTime - os.path.getctime(TName + '.json') > oldTime:
        isOld = True
if isOld:
    print 'It has been a while since you did this...'
    print 'Redownloading PhEDEx files...'
    opts.newDownload = True

if opts.newDownload:                                                # If your starting fresh enough for a new download, walk the PhEDEx file and directory again
    opts.newPhedexAndWalk = True
elif os.path.exists(TName + '.tar.gz'):                             # If you're not downloading again, pull files from the tarball
    print 'Extracting files from tarball...'
    os.system('tar -xvzf ' + TName + '.tar.gz')
if opts.newPhedexAndWalk:                                           
    opts.newWalk   = True
    opts.newPhedex = True

print 'Checking for empty files...'                                 # If anything from the tarball is empty or almost empty, remove it so that things are rerun
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
    print check
    if check['protocol'] == 'direct' and check['element_name'] == 'lfn-to-pfn':
        tfcPath = check['result']
        tfcName = check['path-match']
        print "tfcPath:"
        print tfcPath
        print "tfcName:"
        print tfcName

if tfcPath.split('$')[-1] == '1':                                   # If the format matches, it'll have a /somestuff/$1 at the end
    remove = tfcName.split('+')[-1].split('(.*)')[0]                # which I can just take off and add to the front of the LFN
    if(len(remove) > 0):
        preFix = tfcPath.split(remove)[0:-1]
    else:
        preFix = tfcPath.split('$')[0:-1]
    print 'Looks good...'
    print preFix
else:
    print 'ERROR: Problem with the TFC.'                            # If the format is unexpected, I give up
    exit()

if not os.path.exists(TName + '.json') or opts.newDownload:         # Download JSON file list from PhEDEx if needed or asked for
    print 'Getting file list...'
    os.system('wget --no-check-certificate -O '+TName+'.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node='+TName)
else:
    print 'Already have the file list...'

if not os.path.exists(TName + '_phedex.json') or opts.newPhedex:    # Parse the JSON file if needed to make a new format
    print 'Loading file list. Please wait...'
    inFile = open(TName + '.json')
    inData = json.load(inFile, object_hook = deco._decode_dict)     # This step takes a while
    inFile.close()
    print 'Size of inData: ' + str(sys.getsizeof(inData))

    print 'Skimming PhEDEx output. Please wait...'
    blockList = []
    tempBlock = []
    lastDirectory = ''                                              # Okay, so I want to keep track of every time there's a directory change and make
    lastBlock = ''                                                  # a new list entry of the directory. (There are some duplicate directories, but that's fine)
    for block in inData['phedex']['block']:
        for repl in block['file']:
            if preFix + stripFile(repl['name']) != lastDirectory:   # This is where I spot the directory change
                if len(lastDirectory) > 0:                          # If it's not the first directory, I append the old directory info and reset
                    blockList.append({'dataset':lastBlock,'directory':lastDirectory,'files':tempBlock})           # Information for each directory
                    tempBlock = []
                lastBlock = block['name']                           # After adding, I update the block
                lastDirectory = preFix + stripFile(repl['name'])    # and directory information
            for getTime in repl['replica']:                         # Getting creation time of the replica. Give the way our request is
                if getTime['node'] == TName:                        # done, this step might be unnecessary, but it doesn't take too long
                    try:                                            # If there is no time stored in PhEDEx
                        tempTime = float(getTime['time_create'])    # Store default time of 0.0, so code will check if file is present
                    except:
                        tempTime = 0.0
            tempBlock.append({'file':repl['name'].split('/')[-1],'size':repl['bytes'],'time':tempTime,            # Information for each file
                              'adler32':pullAdler(repl['checksum'])})                                             # is stored here
    blockList.append({'dataset':block['name'],'directory':preFix + stripFile(repl['name']),'files':tempBlock})    # Don't forget the last directory
    del inData                                                      # This is an attempt to free memory. I'm not convinced it's working...
    print 'Writing skimmed file...'
    outParsed = open(TName + '_phedex.json','w')
    outParsed.write(json.dumps(blockList))
    outParsed.close()
    del blockList                                                   # This is an attempt to free memory. I'm not convinced it's working...
    print 'Done with that...'
else:                                                               # If not parsing the JSON file, let the user know
    print 'Using old skimmed file...'

startDir = tfcPath.split('$1')[0]                                   # This is will be the starting location of the walk through the site's existing files

if skipCksm:
    print 'Skipping Checksum (Adler32) calculations...'
else:
    print 'Will calculate Checksum unless old file exists...'
if (not skipCksm and not os.path.exists(TName + '_exists.json')) or (skipCksm and not os.path.exists(TName + '_skipCksm_exists.json')) or opts.newWalk:
    print 'Creating JSON file from your directory...'
    print 'Starting walk...'
    existsList = []                                                 # This will be the list of directories, each with a list of files inside
    tempBlock=[]                                                    # Temp list to store the list of files
    for subDir in subDirs:                                          # This is the list of directories walked through
        subDir = subDir.strip(' ')                                  # If people put spaces in their configuration file, they shouldn't be punished
        for term in os.walk(startDir + subDir):
            if len(term[-1]) > 0:                                   # If the directory has files in it, do the following
                print term[0]
                tempBlock=[]                                        # Reset directory contents
                for aFile in term[-1]: 
                    fullName = term[0]+'/'+aFile
                    if not skipCksm:
                        tempFile = open(fullName)                   # Open file only if calculating checksum
                        asum = 1
                        while True:
                            buffer = tempFile.read(BLOCKSIZE)       # BLOCKSIZE is how much of the file is read at a time, keep this smallish
                            if not buffer:                          # Go until the file is ended
                                break
                            asum = zlib.adler32(str(buffer),asum)   # This calculates the checksum of the buffer
                            if asum < 0:
                                asum += 2**32
                        tempFile.close()
                        cksumStr = str(hex(asum))[2:10]             # Convert to format given by PhEDEx
                        print 'Got checksum...'
                    else:
                        cksumStr = 'Not Checked'                    # If not calculated, still give something to the output file
                    tempBlock.append({'file':aFile,'size':os.path.getsize(fullName),'time':os.path.getmtime(fullName),
                                      'adler32':cksumStr})
                existsList.append({'directory':term[0]+'/','time':os.path.getctime(term[0]),
                                   'files':tempBlock})              # Each directory is added to the full list
    if len(existsList) > 0:                                         # Only do this if the directories wheren't completely empty
        print 'Creating JSON file from directory...'
        if skipCksm:
            outExists = open(TName + '_skipCksm_exists.json','w')
        else:
            outExists = open(TName + '_exists.json','w')
        outExists.write(json.dumps(existsList))                     # Save the list just made to a file
        outExists.close()
        print 'Done with that...'
    else:
        print 'Exists list is empty...'                             # If the list is empty, something went wrong
        exit()                                                      # Kill python
else:                                                               # If the walk isn't asked for or needed, don't do it
    print 'Using old directory JSON file...'

print 'Now comparing the two...'
clearSize = compare.finalCheck(TName,skipCksm)                      # Compares the parsed PhEDEx file and the walk file. See compare.py

print 'Checking for empty files...'
cleanEmpty()                                                        # Clears out any empty or very small files again

print 'Making tarball for storage: ' + TName +'.tar.gz'             # Make tarball for compressed storage
os.system('tar -cvzf ' + TName + '.tar.gz ' + TName + '*.json ' + TName + '*results.txt')
print 'Everything stored in: ' + TName +'.tar.gz'

print 'Elapsed time: ' + str(time() - startTime) + ' seconds'       # Output elapsed time

print '******************************************************************************'
print 'If you run the following command: '
print 'python ClearSite.py -T ' + TName
print 'You will clear ' + str(float(clearSize)/2**30) + ' GB of space.'
print '******************************************************************************'
