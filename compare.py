import json, os

def writeBlock(dataSet,report):                           # Quickly writes the dataset and block name in the output
    report.write('------------------------------------------------------ \n')
    report.write('Dataset: ' + dataSet.split('#')[0] + ' \n')
    report.write('Block  : ' + dataSet.split('#')[1] + ' \n')


def finalCheck(TName,skipCksm):
    firstFile = open(TName + '_phedex.json')              # Loads the JSON file of parsed PhEDEx
    print 'Loading first file...'
    firstData = json.load(firstFile)
    firstFile.close()
    print 'Loaded...'

    if skipCksm:                                          # Then loads the JSON file of what exists in directory
        if not os.path.exists(TName + '_skipCksm_exists.json'):
            print 'Exists file does not exist. No comparison to make...'
            exit()
        secondFile = open(TName + '_skipCksm_exists.json')
    else:
        if not os.path.exists(TName + '_exists.json'):
            print 'Exists file does not exist. No comparison to make...'
            exit()
        secondFile = open(TName + '_exists.json')
    print 'Loading second file...'
    secondData = json.load(secondFile)
    secondFile.close()
    print 'Loaded...'

    if skipCksm:           f                              # Everything will be stored differently when skipping checksum calculations
        print 'Report will be in: ' + TName + '_skipCksm_results.txt'
        report = open(TName + '_skipCksm_results.txt','w')
        report.write('Skipping Checksum (Alder32) comparisons! \n')
    else:
        print 'Report will be in: ' + TName + '_results.txt'
        report = open(TName + '_results.txt','w')
    print 'Started writing...'
    report.write('\nFiles missing at site: \n\n')         # First list files that PhEDEx thinks should be there that aren't
    for aBlock in firstData:                              # For every directory in the parsed list
        foundDir = False                                  # search for a match of each directory in exists list
        aDirectory = aBlock['directory']
        for bBlock in secondData:
            if aBlock['directory'] == bBlock['directory']:      # Here's the directory matching
                foundDir = True
                wroteDataSetName = False                  # Each directory is assumed to be in a different dataset
                for aFile in aBlock['files']:             # For every file in the directory
                    found = False                         # search for a match in the exists list
                    aName = aFile['file']                 # Store this stuff for easy writing later, not comparison
                    aSize = aFile['size']
                    aCksm = aFile['adler32']
                    for bFile in bBlock['files']:
                        if aFile['file'] == bFile['file']:      # Here's the file matching
                            found = True                        # Flag file as found and check if consistent
                            if aFile['size'] == bFile['size'] and (skipCksm or aFile['adler32'] == bFile['adler32']):
                                break
                            else:
                                if not wroteDataSetName:        # If dataset name hasn't been written yet, write it in report
                                    wroteDataSetName = True     # This prevents spamming report with same dataset name
                                    writeBlock(aBlock['dataset'],report)
                                report.write(aDirectory + aName + ' has incorrect size or checksum: PhEDEx -- '+str(aCksm)+' '+str(aSize)+'; Site -- '+str(bCksm)+' '+str(bSize)+' \n')
                                break

                    if not found:                         # If file is not found after looping through exists directory write it in report
                        if not os.path.exists(aDirectory + aName):
                            if not wroteDataSetName:
                                wroteDataSetName = True
                                writeBlock(aBlock['dataset'],report)
                            report.write(aDirectory + aName + ' \n')
                        else:                             # If file was not in exists list, but does exist, I didn't search everywhere
                            if not wroteDataSetName:
                                wroteDataSetName = True
                                writeBlock(aBlock['dataset'],report)
                            report.write(aDirectory + aName + ' was not in a searched directory. \n')
        if not foundDir:                                  # If there's no match for the entire directory, note that in report
            writeBlock(aBlock['dataset'],report)
            report.write('No files were found in ' + aDirectory + ' \n')

    report.write('\n')
    report.write('File not in PhEDEx: \n\n')              # Switching to files that are at site, but not in PhEDEx
    clearSize = 0                                         # Store how much space would be cleared by deleting these files
    for aBlock in secondData:                             # Again, look for directory matching
        aDirectory = aBlock['directory']
        bDirectoryList = []                               # It's possible to have duplicate directories in PhEDEx file
        for bBlock in firstData:                          # if the same block switches back and forth between directories
            wroteDataSetName = False
            if aBlock['directory'] == bBlock['directory']:      # Find all of the directories that match
               bDirectoryList.append(bBlock)
        if len(bDirectoryList) > 0:                       # If there are directories that match, search for individual file matches
            for aFile in aBlock['files']:
                found = False
                aName = aFile['file']
                for bBlock0 in bDirectoryList:
                    for bFile in bBlock0['files']:
                        if found:
                            break                               # If file is found, then you can stop searching for it
                        if aFile['file'] == bFile['file']:      # Here's the file comparison
                            found = True
                            break
                if not found:                                   # If there was no match for the file, note in the report
                    if not wroteDataSetName:                    # Can write the dataset name for matching directories
                        wroteDataSetName = True
                        writeBlock(bDirectoryList[0]['dataset'],report)   # Note this assumes all files in same directory are from same block
                    report.write(aDirectory + aName + ' \n')
                    clearSize = clearSize + aFile['size']       # Add to the space that would be cleared out
        else:                                                                  # If entire directory is not matched, note this in report
            report.write('PhEDEx expects nothing in ' + aDirectory + ' \n')    # This is the flag the whole directory should be removed
            for aFile in aBlock['files']:                       # Find the space that would be cleared out for the whole directory
                clearSize = clearSize + aFile['size']
    report.write('\n')
    # Stick some useful instructions at the end of the report
    report.write('****************************************************************************** \n')
    report.write('If you run the following command:  \n')
    report.write('python ClearSite.py -T ' + TName + ' \n')
    report.write('You will clear ' + str(float(clearSize)/2**30) + ' GB of space. \n')
    report.write('****************************************************************************** \n')
    report.write('Run the following command to not pause for every directory:  \n')
    report.write('python ClearSite.py --fast -T ' + TName ' \n')
    report.write('Run the following command to just give output of what will be deleted, \n')
    report.write('witout actually removing anything:  \n')
    report.write('python ClearSite.py --safe -T ' + TName ' \n')
    report.write('or:  \n')
    report.write('python ClearSite.py --safe --fast -T ' + TName ' \n')

    report.close()
    return clearSize
