import os, sys, pwd, grp
from optparse import OptionParser
from time import sleep
import ConfigParser

parser = OptionParser()

parser.add_option('-c',help='Names a configuration file. If configuration file is present, all other options are ignored.',
                  dest='configName',action='store',metavar='<name>')
parser.add_option('-T',help='Name of the site. Input is used to find files <site>.json and <site>_tfc.json.',
                  dest='TName',action='store',metavar='<name>')
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
    opts.exceptions = config.getboolean('ClearSite','UseExceptionsList')

TName = opts.TName                     # Name of the site is stored here

if opts.exceptions:
    if os.path.exists(TName + '_exceptions_missing.txt'):    # Here, pull out the exceptions list
        listOfFiles = open(TName + '_exceptions_missing.txt')
    else:
        print 'Missing exceptions file... exiting.'          # If no results, give up
        exit()
else:
    if os.path.exists(TName + '_skipCksm_missing.txt'):      # Here, I am looking for a results file
        listOfFiles = open(TName + '_skipCksm_missing.txt')  # to read from. Skipping checksum is the 
    elif os.path.exists(TName + '_missing.txt'):             # default, but otherwise is okay.
        listOfFiles = open(TName + '_missing.txt')
    else:
        print 'Missing results file... exiting.'             # If no results, give up
        exit()

startedOrphan = False                  # First part of results is of missing files. Ignore those entries
exceptionList = []                     # Initialize a list to store exceptions from trying to delete things
for line in listOfFiles.readlines():
    copyLine = False
    if startedOrphan and len(line) > 2:
        if line.startswith('******************************************************************************'):
            startedOrphan = False                            # Ignore any other lines
            print 'Copying files ended'
            break
        elif line.startswith('/'):
            copyFile = line.split()[0]
            if not os.path.isfile(line.split()[0]):              # Check that the file isn't already there
                copyLine = True
            elif "has incorrect size or checksum" in line:
                try:
                    tempSize = os.path.getsize(copyFile)
                    if not tempSize == int(line.split('size')[1].split(';')[0]):
                        copyLine = True
                except:
                    copyLine = True
            if copyLine:
                print '******************************************************************************'
                print 'Trying to copy file'
                print copyFile
                print '******************************************************************************'
                directory = copyFile.rsplit('/'+directory.split('/')[-2]+'/',1)[0] + '/'
                if os.path.isdir(directory):
                    try:
                        dirStat = os.stat(directory)
                        owner = pwd.getpwuid(dirStat.st_uid)[0]
                        group = grp.getgrgid(dirStat.st_gid)[0]
                        perms = oct(test.st_mode)[-3:]
                        copyCall = copyFile.split('/store/')[1];
                        os.system('echo xrdcp root://cmsxrootd.fnal.gov//store/' + copyCall + " " + copyFile)
                        os.system('echo chmod ' + str(perms) + ' ' + copyFile)
                        os.system('echo chown ' + owner + ':' + group + ' ' + copyFile)
                    except:
                        print '*********************************************'
                        print '*    Exception thrown, file not copied      *'
                        print '*********************************************'
                        exceptionList.append(line)
                else:
                    print '## DIRECTORY DOESN\'T EXIST! ##'
                    print 'Skipping: ' + copyFile
    if line == 'Files missing at site: \n':                  # This is the flag to start listing files that should be removed
        startedOrphan = True

if len(exceptionList) > 0:             # If exceptions were thrown, write an exceptions file
    exceptionsFile = open(TName + '_exceptions_missing.txt','w')
    exceptionsFile.write('\n\n')
    exceptionsFile.write('Files missing at site: \n')        # Basically just copies the format of the results file
    exceptionsFile.write('\n\n')
    for line in exceptionList:                                             # But it should be much smaller
        exceptionsFile.write(line)
    exceptionsFile.write('****************************************************************************** \n')
    exceptionsFile.close()
    print '****************************************************************************************'
    print 'Exceptions were thrown during operation, preventing removal of all files.'
    print 'If this was caused by missing write permissions, try running the following.'
    print ''
    print 'sudo python FillSite.py -e -T ' + TName
    print ''
    print 'Or you could just run with the -e option as root.'
    print '****************************************************************************************'
