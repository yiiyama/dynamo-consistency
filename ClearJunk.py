import os
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')

(opts,args) = parser.parse_args()

mandatories = ['TName']
for m in mandatories:                  # If the name of the site is not specified, end the program.
    if not opts.__dict__[m]:
        print '\nMandatory option is missing\n'
        parser.print_help()
        exit(-1)

TName = opts.TName                     # Name of the site is stored here

if os.path.exists(TName + '_skipCksm_results.txt'):
    listOfFiles = open(TName + '_skipCksm_results.txt')
elif os.path.exists(TName + '_results.txt'):
    listOfFiles = open(TName + '_results.txt')
else:
    print 'Missing results file... exiting.'
    exit()

startedOrphan = False
for line in listOfFiles.readlines():
    if startedOrphan:
        if line.endswith('.root \n'):
            os.remove(line.split()[0])
        if line.startswith('PhEDEx expects nothing in '):
            directory = line.split()[4]
            presentFiles = os.listdir(directory)
            for aFile in presentFiles:
                if os.path.isfile(directory + aFile):
                    os.remove(directory + aFile)
            while True:
                if not os.listdir(directory):
                    os.rmdir(directory)
                    directory = directory.split(directory.split('/')[-1])[0]
                else:
                    break
    if line == 'File not in PhEDEx: \n':
        startedOrphan = True
