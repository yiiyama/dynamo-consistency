import os, sys
from optparse import OptionParser
from time import sleep

parser = OptionParser()

parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--safe',help='Does not try to remove the files. Just prints out results instead.',
                  dest='safe',action='store_true')

(opts,args) = parser.parse_args()

mandatories = ['TName']
for m in mandatories:                  # If the name of the site is not specified, end the program.
    if not opts.__dict__[m]:
        print '\nMandatory option is missing\n'
        parser.print_help()
        exit(-1)

TName = opts.TName                     # Name of the site is stored here
remove = not opts.safe                 # Remove files if not run in safe mode

if os.path.exists(TName + '_skipCksm_results.txt'):
    listOfFiles = open(TName + '_skipCksm_results.txt')
elif os.path.exists(TName + '_results.txt'):
    listOfFiles = open(TName + '_results.txt')
else:
    print 'Missing results file... exiting.'
    exit()

startedOrphan = False
for line in listOfFiles.readlines():
    if startedOrphan and len(line) > 2:
        if len(line.split('.')) > 1:
            if os.path.isfile(os.remove(line.split()[0])):
                print 'Removing file ' + line.split()[0]
                if remove:
                    os.remove(line.split()[0])
        if line.startswith('PhEDEx expects nothing in '):
            directory = line.split()[4]
            print '******************************************************************************'
            print 'Removing directory'
            print directory
            print '******************************************************************************'
            print 'Pausing for 2 seconds before deleting.'
            print 'Hit Ctrl-C to interrupt.'
            sleep(2)
            presentFiles = os.listdir(directory)
            for aFile in presentFiles:
                if os.path.isfile(directory + aFile):
                    print 'Removing file ' + directory + aFile
                    if remove:
                        os.remove(directory + aFile)
            while True:
                if (not os.listdir(directory)) or ((not remove) and len(os.listdir(directory)) == 1):
                    print 'Removing directory ' + directory
                    if remove:
                        os.rmdir(directory)
                    directory = directory.split(directory.split('/')[-2])[0]
                else:
                    break
    if line == 'File not in PhEDEx: \n':
        startedOrphan = True
