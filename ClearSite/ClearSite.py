#! /usr/bin/python

import os, sys
from optparse import OptionParser
from time import sleep
import ConfigParser

parser = OptionParser()

parser.add_option('-c',help='Names a configuration file. If configuration file is present, all other options are ignored.',
                  dest='configName',action='store',metavar='<name>')
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
parser.add_option('--safe',help='Does not try to remove the files. Just prints out results instead.',
                  dest='safe',action='store_true')
parser.add_option('--fast',help='Does not wait for two second for each directory. Only use if you trust me.',
                  dest='fast',action='store_true')
parser.add_option('-e',help='Uses the exceptions list to try to remove directories again.',
                  dest='exceptions',action='store_true')

(opts,args) = parser.parse_args()

if not opts.__dict__['configName']:                                 # User must specify a configuration file
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
    opts.TName      = config.get('General','SiteName')
    opts.safe       = config.getboolean('ClearSite','DontRemoveFiles')
    opts.fast       = config.getboolean('ClearSite','NoPausing')
    opts.exceptions = config.getboolean('ClearSite','UseExceptionsList')

TName = opts.TName                     # Name of the site is stored here
remove = not opts.safe                 # Remove files if not run in safe mode

if opts.exceptions:
    if os.path.exists(TName + '_exceptions.txt'):            # Here, pull out the exceptions list
        listOfFiles = open(TName + '_exceptions.txt')
    else:
        print 'Missing exceptions file... exiting.'          # If no results, give up
        exit()
else:
    if os.path.exists(TName + '_skipCksm_removable.txt'):      # Here, I am looking for a results file
        listOfFiles = open(TName + '_skipCksm_removable.txt')  # to read from. Skipping checksum is the 
    elif os.path.exists(TName + '_removable.txt'):             # default, but otherwise is okay.
        listOfFiles = open(TName + '_removable.txt')
    elif os.path.exists(TName + '.tar.gz'):                  # It's possible for results to be in the tarball
        print 'Extracting files from tarball...'
        os.system('tar -xvzf ' + TName + '.tar.gz')
    else:
        print 'Missing results file... exiting.'             # If no results, give up
        exit()

startedOrphan = False                  # First part of results is of missing files. Ignore those entries
exceptionList = []                     # Initialize a list to store exceptions from trying to delete things
for line in listOfFiles.readlines():
    if startedOrphan and len(line) > 2:
        if line.startswith('********'):                      # Stars occur at the end of the file list
            startedOrphan = False                            # Ignore any other lines
            print 'Clearing files ended'
            break
        if os.path.isdir(line.split()[0]):             # This is in results if whole directory should be removed
            directory = line.split()[0]
            print '******************************************************************************'
            print 'Removing directory'
            print directory
            print '******************************************************************************'
            if not opts.fast:                                     # Delay for user to react can be ignored by options
                print 'Pausing for 2 seconds before deleting.'
                print 'Hit Ctrl-C to interrupt.'
                sleep(2)
            if os.path.isdir(directory):                     # Make sure directory exists
                presentFiles = os.listdir(directory)         # Remove the files one at a time
                for aFile in presentFiles:
                    if os.path.isfile(directory + aFile):
                        print 'Removing file ' + directory + aFile
                        if remove:
                            try:
                                os.remove(directory + aFile)
                            except:
                                print '*********************************************'
                                print '*    Exception thrown, file not removed     *'
                                print '*********************************************'
                                exceptionList.append(line)
                while True:                              # Then iteratively remove empty directories left behind
                    if (not os.listdir(directory)) or ((not remove) and len(os.listdir(directory)) == 1):
                        print 'Removing directory ' + directory
                        if remove:
                            try:
                                os.rmdir(directory)
                            except:
                                print '*********************************************'
                                print '*  Exception thrown, directory not removed  *'
                                print '*********************************************'
                                exceptionList.append(line)
                                break
                        directory = directory.rsplit('/'+directory.split('/')[-2]+'/',1)[0] + '/'
                        if not os.path.isdir(directory):
                            break
                    else:                                    # Stop if you reach a not empty directory
                        break
            else:                      # If the directory is not there, skip it. Possibly results file hasn't been updated
                print 'Directory has already been removed.'
        elif os.path.isfile(line.split()[0]):                # If the line is not a directory, but it is a file, remove it
            print 'Removing file ' + line.split()[0]
            if remove:
                try:
                    os.remove(line.split()[0])
                except:
                    print '*********************************************'
                    print '*    Exception thrown, file not removed     *'
                    print '*********************************************'
                    exceptionList.append(line)
    if line == 'Files not in PhEDEx (to be removed): \n':                  # This is the flag to start listing files that should be removed
        startedOrphan = True

if len(exceptionList) > 0:             # If exceptions were thrown, write an exceptions file
    exceptionsFile = open(TName + '_exceptions.txt','w')
    exceptionsFile.write('\n\n')
    exceptionsFile.write('Files not in PhEDEx (to be removed): \n')        # Basically just copies the format of the results file
    exceptionsFile.write('\n\n')
    for line in exceptionList:                                             # But it should be much smaller
        exceptionsFile.write(line)
    exceptionsFile.write('****************************************************************************** \n')
    exceptionsFile.close()
    print '****************************************************************************************'
    print 'Exceptions were thrown during operation, preventing removal of all files.'
    print 'Inspect ' + TName + '_exceptions.txt to ensure that you want to remove those files.'
    print 'If you are sure you want to remove those files, run the following command:'
    print ''
    print 'sudo python ClearSite.py -e -T ' + TName
    print ''
    print 'Or you could just run with the -e option as root.'
    print '****************************************************************************************'
