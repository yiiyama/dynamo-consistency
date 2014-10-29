import os, sys
from optparse import OptionParser
from time import sleep

parser = OptionParser()

parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--safe',help='Does not try to remove the files. Just prints out results instead.',
                  dest='safe',action='store_true')
parser.add_option('--fast',help='Does not wait for two second for each directory. Only use if you trust me.',
                  dest='fast',action='store_true')

(opts,args) = parser.parse_args()

mandatories = ['TName']
for m in mandatories:                  # If the name of the site is not specified, end the program.
    if not opts.__dict__[m]:
        print '\nMandatory option is missing\n'
        parser.print_help()
        exit(-1)

TName = opts.TName                     # Name of the site is stored here
remove = not opts.safe                 # Remove files if not run in safe mode

if os.path.exists(TName + '_skipCksm_results.txt'):      # Here, I am looking for a results file
    listOfFiles = open(TName + '_skipCksm_results.txt')  # to read from. Skipping checksum is the 
elif os.path.exists(TName + '_results.txt'):             # default, but otherwise is okay.
    listOfFiles = open(TName + '_results.txt')
elif os.path.exists(TName + '.tar.gz'):                  # It's possible for results to be in the tarball
    print 'Extracting files from tarball...'
    os.system('tar -xvzf ' + TName + '.tar.gz')
else:
    print 'Missing results file... exiting.'             # If no results, give up
    exit()

startedOrphan = False                  # First part of results is of missing files. Ignore those entries
for line in listOfFiles.readlines():
    if startedOrphan and len(line) > 2:
        if line.startswith('********'):                  # Stars are only at the end of the file list
            startedOrphan = False                        # Ignore any other lines
            print 'Clearing files ended'
            break
        if line.startswith('PhEDEx expects nothing in '):         # This is in results if whole directory should be removed
            directory = line.split()[4]
            print '******************************************************************************'
            print 'Removing directory'
            print directory
            print '******************************************************************************'
            if not opts.fast:                                     # Delay for user to react can be ignored by options
                print 'Pausing for 2 seconds before deleting.'
                print 'Hit Ctrl-C to interrupt.'
                sleep(2)
            if os.path.isdir(directory):                 # Make sure directory exists
                presentFiles = os.listdir(directory)     # Remove the files one at a time
                for aFile in presentFiles:
                    if os.path.isfile(directory + aFile):
                        print 'Removing file ' + directory + aFile
                        if remove:
                            os.remove(directory + aFile)
                while True:                              # Then iteratively remove empty directories left behind
                    if (not os.listdir(directory)) or ((not remove) and len(os.listdir(directory)) == 1):
                        print 'Removing directory ' + directory
                        if remove:
                            os.rmdir(directory)
                        directory = directory.split(directory.split('/')[-2])[0]
                    else:                                # Stop if you reach a not empty directory
                        break
            else:                      # If the directory is not there, skip it. Possibly results file hasn't been updated
                print 'Directory has already been removed.'
        elif os.path.isfile(line.split()[0]):            # If the line is not a directory, but it is a file, remove it
            print 'Removing file ' + line.split()[0]
            if remove:
                os.remove(line.split()[0])
    if line == 'File not in PhEDEx: \n':                 # This is the flag to start listing files that should be removed
        startedOrphan = True
