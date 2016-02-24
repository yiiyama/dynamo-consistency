import os, json, zlib, urllib2, sys
from time import time
import compare, LFN2PFNConverter
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
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_lfn2pfn.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--do-checksum',help='Do checksum calculations and comparison.',action='store_true',dest='doCksm')
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
    opts.doCksm            = config.getboolean('ConsistencyCheck','DoChecksum')
    opts.newPhedexAndWalk  = config.getboolean('UseCache','ParsePhEDExAndDir')
    opts.newPhedex         = config.getboolean('UseCache','ParsePhEDEx')
    opts.newWalk           = config.getboolean('UseCache','ParseDir')

TName = opts.TName                                                  # Name of the site is stored here
skipCksm = not opts.doCksm                                          # Skipping checksums became the default

startTime = time()                                                  # Start timing for a final readout of the run time
oldTime = 604800                                                    # If the PhEDEx file hasn't been downloaded for a week, quit the job

isOld = False
if os.path.exists(TName + '_phedex.json'):
    if startTime - os.path.getctime(TName + '_phedex.json') > oldTime:
        isOld = True
elif os.path.exists('../Cache/' + TName + '/' + TName + '_phedex.json'):
    if startTime - os.path.getctime('../Cache/' + TName + '/' + TName + '_phedex.json') > oldTime:
        isOld = True
else:
    print 'Missing PhEDEx file.'
    exit()
if isOld:
    print 'It has been a while since you did this...'
    print 'Redownload PhEDEx files...'
    exit()

if opts.newPhedexAndWalk:                                           
    opts.newWalk   = True
    opts.newPhedex = True

print 'Checking for empty files...'                                 # If anything from the tarball is empty or almost empty, remove it so that things are rerun
cleanEmpty()

if not os.path.exists(TName + '_lfn2pfn.json'):
    print 'Missing TFC...'
    exit()

prefix   = LFN2PFNConverter.GetPrefix(TName)                        # Get the file prefix using the TFC file
startDir = prefix + '/store/'

if skipCksm:
    print 'Skipping Checksum (Adler32) calculations...'
else:
    print 'Will calculate Checksum unless old file exists...'
if (not skipCksm and not os.path.exists(TName + '_exists.json')) or (skipCksm and not os.path.exists(TName + '_skipCksm_exists.json')) or opts.newWalk:
    print 'Creating JSON file from your directory...'
    print 'Starting walk...'
    existsList = []                                                 # This will be the list of directories, each with a list of files inside
    print startDir
    for subDir in subDirs:                                          # This is the list of directories walked through
        subDir = subDir.strip(' ')                                  # If people put spaces in their configuration file, they shouldn't be punished
        print startDir + subDir
        tempBlock=[]                                                # Temp list to store the list of files
        lastDirectory = ''
        for term in os.walk(startDir + subDir):
            directoryName = term[0].rsplit(term[0].split('/')[-1],1)[0]
            if len(term[-1]) > 0:                                   # If the directory has files in it, do the following
                if directoryName != lastDirectory:
                    if len(tempBlock) > 0:
                        existsList.append({'directory':lastDirectory,'time':os.path.getctime(lastDirectory),
                                           'files':tempBlock})      # Each directory is added to the full list
                    tempBlock=[]                                    # Reset directory contents
                    lastDirectory = directoryName
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
                    try:
                        aSize = os.path.getsize(fullName)           # There's no real reason I could expect this to be a problem
                        aTime = os.path.getmtime(fullName)          # But apparently it can throw an error...
                    except:
                        aSize = 'ERROR ACCESSING'
                        aTime = 'ERROR ACCESSING'
                    tempBlock.append({'file':term[0].split('/')[-1]+'/'+aFile,'size':aSize,'time':aTime,'adler32':cksumStr})
        if len(tempBlock) > 0:
            existsList.append({'directory':lastDirectory,'time':os.path.getctime(lastDirectory),
                               'files':tempBlock})                  # Each directory is added to the full list

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
        print 'Checking for empty files...'
        cleanEmpty()                                                # Clears out any empty or very small files again
        print 'Making tarball for storage: ' + TName +'.tar.gz'     # Make tarball for compressed storage
        os.system('tar -cvzf ' + TName + '.tar.gz ' + TName + '*.json')
        print 'Everything stored in: ' + TName +'.tar.gz'
        exit()                                                      # Kill python
else:                                                               # If the walk isn't asked for or needed, don't do it
    print 'Using old directory JSON file...'

print 'Now comparing the two...'
sizes =  compare.finalCheck(TName,skipCksm)                        # Compares the parsed PhEDEx file and the walk file. See compare.py
missingSize = sizes[0]
clearSize = sizes[1]

print 'Checking for empty files...'
cleanEmpty()                                                        # Clears out any empty or very small files again

print 'Making tarball for storage: ' + TName +'.tar.gz'             # Make tarball for compressed storage
os.system('tar -cvzf ' + TName + '.tar.gz ' + TName + '*.json ' + TName + '*.txt')
print 'Everything stored in: ' + TName +'.tar.gz'

print 'Elapsed time: ' + str(time() - startTime) + ' seconds'       # Output elapsed time

print '******************************************************************************'
fmt1 = '{0:<42} {1:>13}'
print 'Space used in searched areas:',str(float(sizes[2])/2**40) + ' TB.'
print 'That should be used, according to PhEDEx:',str(float(sizes[3])/2**40) + ' TB.'
print '******************************************************************************'
print 'You are missing ' + str(float(missingSize)/2**30) + ' GB worth of files.'
print '******************************************************************************'
print 'If you run the following command: '
print 'python ClearSite.py -T ' + TName
print 'You will clear ' + str(float(clearSize)/2**30) + ' GB of space.'
print '******************************************************************************'
