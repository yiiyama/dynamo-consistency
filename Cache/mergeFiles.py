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
    inData = json.load(inFile)                                  # This step takes a while
    inFile.close()
except:
    print 'File list wasn\'t successfully loaded.'
    print 'Exiting...'
    exit()

deletedList = []

for block in inData['block']:
    deletedList.append([block['name'],block['time']])

