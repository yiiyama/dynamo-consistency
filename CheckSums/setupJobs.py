import os, json, zlib, urllib2, sys
import deco
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-T', help='Name of the site.', dest='TName', action='store', metavar='<name>')  # Determines where to get Exists list

(opts,args) = parser.parse_args()

firstFile = open('Cache/' + opts.TName + '/' + opts.TName + '_skipCksm_exists.json')               # Get list of existing files
firstData = json.load(firstFile)
firstFile.close()

countFiles  = 0             # Make sure number of files in a job
FilesLimit  = 100           # does not exceed some given number
checkedDirs = []            # Don't look at same directory twice
tempList    = []            # Make a list for each job
masterList  = []            # Make a list of these job lists
for aBlock in firstData:
    checked = False
    for dir in checkedDirs:                             # First check for duplicates
        if dir == aBlock['directory']:
            checked = True
    if not checked:
        countThis = 0
        checkedDirs.append(aBlock['directory'])         # If not checked, make note of directory
        for aFile in aBlock['files']:                   # Count the files in the directory
            countThis = countThis + 1
        if countFiles + countThis > FilesLimit and len(tempList) > 0:       # If there are too many files for this job, stop
            masterList.append(tempList)
            tempList = []
            tempList.append(aBlock['directory'])
            countFiles = countThis
        else:
            tempList.append(aBlock['directory'])        # Otherwise, proceed as normal
            countFiles = countFiles + countThis
masterList.append(tempList)

jobNum = 0
for minilist in masterList:
    jobNum = jobNum + 1
    jobConfig = open('config' + str(jobNum) + '.cfg','w')
    jobConfig.write("[General]\n")
    jobConfig.write("# Name of the computing site being checked; Needed to download file list from PhEDEx\n")
    jobConfig.write("SiteName = " + opts.TName + "\n")
    jobConfig.write("\n")
    jobConfig.write("########################################################################################################################\n")
    jobConfig.write("\n")
    jobConfig.write("# Configuration options for ConsistencyCheck.py\n")
    jobConfig.write("[ConsistencyCheck]\n")
    jobConfig.write("\n")
    jobConfig.write("# This is the list of default directories that are checked when not using a configuration file\n")
    jobConfig.write("Directories = ")
    firstTerm = True
    for term in minilist:
        if firstTerm:
            firstTerm = False
        else:
            jobConfig.write(", ")
        jobConfig.write(term.split("/store/")[1].rstrip('/'))
    jobConfig.write("\n")
    jobConfig.close()
