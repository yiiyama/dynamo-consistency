import tarfile, os
from time import time
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

if not opts.__dict__['TName']: # or give a site name
    print ''
    parser.print_help()        # Otherwise exit the program
    print ''
    print '******************************************************************'
    print ''
    print 'You did not give a site name or specify a configuration file!'
    print 'Give a site name with -T'
    print ''
    print '******************************************************************'
    exit(-1)

TName = opts.TName             # Name of the site is stored here

startTime = time()             # Start timing for a final readout of the run time
oldTime = 5*86400              # If the PhEDEx file hasn't been downloaded for a five days, redownload everything

isOld  = True
hasTFC = False
if os.path.exists(TName+'/'+TName + '.tar.gz'):                         # Check for existence of Cache
    try:
        theFile = tarfile.open(TName+'/'+TName + '.tar.gz','r|gz')          # Open the file if it exists
        names = theFile.getnames()                                          # Store the list of files
        for fileName in names:                                              # Check for TFC and json file
            if fileName == TName+'_lfn2pfn.json':
                hasTFC = True
            elif fileName == TName+'.json':
                if startTime - theFile.getmember(TName+'.json').mtime < oldTime:  # Make sure PhEDEx file is not old
                    isOld = False
        theFile.close()
    except:
        os.system('rm '+TName+'/'+TName+'.tar.gz'
else:
    if not os.path.exists(TName):
        os.makedirs(TName)

if not hasTFC ^ isOld:                                                  # If we only need one of the files,
    print 'Extracting files from tarball...'                            # extract that one to avoid overwriting
    if hasTFC:
        os.system('tar -xvzf ' + TName+'/'+TName + '.tar.gz ' + TName + '_lfn2pfn.json')
    else:
        os.system('tar -xvzf ' + TName+'/'+TName + '.tar.gz ' + TName + '.json')

if not hasTFC:                                                          # If missing TFC, get it
    print 'Getting TFC...'
    os.system('wget --no-check-certificate -O '+TName+'_lfn2pfn.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?node='+TName+'\&protocol=direct\&lfn=/store/data/test.root')
#    os.system('wget --no-check-certificate -O '+TName+'_tfc.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node='+TName)
    count = 0
    while cleanEmpty():
        print 'Trying again...'
        os.system('wget --no-check-certificate -O '+TName+'_lfn2pfn.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?node='+TName+'\&protocol=direct\&lfn=/store/data/test.root')
        count = count + 1
        if count > 5:
            break
if isOld:                                                               # If file list is missing or old, get it                                                
    print 'Getting file list...'
    os.system('wget --no-check-certificate -O '+TName+'.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node='+TName)
    count = 0
    while cleanEmpty():
        print 'Trying again...'
        os.system('wget --no-check-certificate -O '+TName+'.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node='+TName)
        count = count + 1
        if count > 2:
            os.system('touch '+TName+'/flag')                           # Flag here for now if there was no success
            break

if not hasTFC or isOld:                                                 # If downloads were necessary, make the tarball
    os.system('tar -cvzf ' + TName+'/'+TName + '.tar.gz ' + TName + '_lfn2pfn.json ' + TName + '.json')
    os.system('rm ' + TName + '_lfn2pfn.json ' + TName + '.json')







