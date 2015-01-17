import json, os,time

def writeBlock(dataSet,report):                                                # Quickly writes the dataset and block name in the output
    report.write('------------------------------------------------------ \n')
    report.write('Dataset: ' + dataSet.split('#')[0] + ' \n')
    report.write('Block  : ' + dataSet.split('#')[1] + ' \n')


def finalCheck(TName,skipCksm):
    currentTime = time.time()
    cutTime = 1512000                                                          # Ignore files that are less than 2.5 weeks old
    timeTolerance = 3600                                                       # If file creation is more than an hour out of sync with PhEDEx, flag for checksum
    firstFile = open(TName + '_phedex.json')                                   # Loads the JSON file of parsed PhEDEx
    print 'Loading first file...'
    firstData = json.load(firstFile)
    firstFile.close()
    print 'Loaded...'

    if skipCksm:                                                               # Then loads the JSON file of what exists in directory
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
    if skipCksm:                                                               # Everything will be stored differently when skipping checksum calculations
        print 'Report will be in: ' + TName + '_skipCksm_results.txt'
        report = open(TName + '_skipCksm_results.txt','w')
        report.write('Skipping Checksum (Alder32) comparisons! \n')
    else:
        print 'Report will be in: ' + TName + '_results.txt'
        report = open(TName + '_results.txt','w')
    print 'Started writing...'
    shouldBeSpace = 0                                                          # This where we store how much memory should be used
    missingSize = 0                                                            # This is where we store how much memory we are missing
    newBlocks = []                                                             # Get ready to store new blocks that should be skipped
    print '***************************************************'
    print ' Checking if all PhEDEx-recorded files are present '
    print '***************************************************'
    report.write('\nFiles missing at site: \n\n')                              # First list files that PhEDEx thinks should be there that aren't
    for aBlock in firstData:                                                   # For every directory in the parsed list
        newDir = False
        for aFile in aBlock['files']:                                          # For every file in the directory
            try:
                shouldBeSpace = shouldBeSpace + int(aFile['size'])             # Calculate how much space should be used according to PhEDEx
            except:
                print "Missing file size for " + aFile['file']
            if (currentTime - aFile['time']) < cutTime:                        # If any file is less than 2.5 weeks old, skip the block for now
                newDir = True
        if newDir:
            newBlocks.append(aBlock['dataset'])                                # Store the block name for future skipping
            print 'Skipping block ' + aBlock['dataset'] + ' because it is new.'
    for aBlock in firstData:                                                   # For every directory in the parsed list
        newBlock = False
        for skipBlock in newBlocks:                                            # Check that this block should not be skipped because it is new
            if aBlock['dataset'] == skipBlock:
                newBlock = True
        if newBlock:
            continue
        foundDir = False                                                       # search for a match of each directory in exists list
        aDirectory = aBlock['directory']
        for bBlock in secondData:
            if aBlock['directory'] == bBlock['directory']:                     # Here's the directory matching
                foundDir = True
                wroteDataSetName = False                                       # Each directory is assumed to be in a different dataset
                for aFile in aBlock['files']:                                  # For every file in the directory
                    found = False                                              # search for a match in the exists list
                    aName = aFile['file']                                      # Store this stuff for easy writing later, not comparison
                    aSize = aFile['size']
                    aCksm = aFile['adler32']
                    for bFile in bBlock['files']:
                        if aFile['file'] == bFile['file']:                     # Here's the file matching
                            found = True                                       # Flag file as found and check if consistent
                            if aFile['size'] == bFile['size'] and (skipCksm or aFile['adler32'] == bFile['adler32']):
                                break
                            else:
                                if not wroteDataSetName:                       # If dataset name hasn't been written yet, write it in report
                                    wroteDataSetName = True                    # This prevents spamming report with same dataset name
                                    writeBlock(aBlock['dataset'],report)
                                bSize = bFile['size']
                                bCksm = bFile['adler32']
                                report.write(aDirectory + aName + ' has incorrect size or checksum: PhEDEx -- chksm:'+str(aCksm)+' size:'+str(aSize)+'; Site -- chksm:'+str(bCksm)+' size:'+str(bSize)+' \n')
                                missingSize = missingSize + int(aSize)         # If the checksum is wrong, that means the file should basically be replaced
                                break

                    if not found:                                              # If file is not found after looping through exists directory write it in report
                        if not os.path.exists(aDirectory + aName):
                            if not wroteDataSetName:
                                wroteDataSetName = True
                                writeBlock(aBlock['dataset'],report)
                            report.write(aDirectory + aName + ' \n')
                            missingSize = missingSize + int(aSize)             # File is missing
                        else:                                                  # If file was not in exists list, but does exist, I didn't search everywhere
                            if not wroteDataSetName:
                                wroteDataSetName = True
                                writeBlock(aBlock['dataset'],report)
                            report.write(aDirectory + aName + ' was not in a searched directory. \n')
        if not foundDir:                                                       # If there's no match for the entire directory, note that in report
            wasSearched = True                                                 # First check that the directory is one that was searched by ConsistencyCheck
            if os.path.exists(aDirectory):                                     # If there's a directory there, then there might be no problem
                if len(os.listdir(aDirectory)) > 0:                            # Check to see if directory is empty
                    wasSearched = False                                        # Directory wasn't searched
            if wasSearched:
                writeBlock(aBlock['dataset'],report)                           # Note the block name
                report.write('No files were found in ' + aDirectory + ' \n')   # If there is no directory where there should be, this might be a problem
                for aFile in aBlock['files']:                                  # List the files that should be included
                    report.write(aDirectory + aFile['file'] + ' \n')            
                    aSize = aFile['size']
                    missingSize = missingSize + int(aSize)                     # File is missing

    print '*********************************************'
    print ' Checking if all present files are in PhEDEx '
    print '*********************************************'
    report.write('\nFiles not in PhEDEx: \n\n')                                 # Switching to files that are at site, but not in PhEDEx
    isUsed = 0                                                                  # Store how much space is used
    clearSize = 0                                                               # Store how much space would be cleared by deleting these files
    for aBlock in secondData:                                                   # Again, look for directory matching
        if (currentTime - aBlock['time']) < cutTime:                            # If the directory is less than 2.5 weeks old, skip it
            print 'Skipping the directory ' + aBlock['directory'] + ' because it is new.'
            for aFile in aBlock['files']:
                try:
                    isUsed = isUsed + int(aFile['size'])                        # Add to the space that is being used
                except:
                    print "Missing file size for " + aFile['file']
            continue
        aDirectory = aBlock['directory']
        bDirectoryList = []                                                     # It's possible to have duplicate directories in PhEDEx file
        for bBlock in firstData:                                                # if the same block switches back and forth between directories
            wroteDataSetName = False
            if aBlock['directory'] == bBlock['directory']:                      # Find all of the directories that match
               bDirectoryList.append(bBlock)
        if len(bDirectoryList) > 0:                                             # If there are directories that match, search for individual file matches
            for aFile in aBlock['files']:
                try:
                    isUsed = isUsed + int(aFile['size'])                        # Add to the space that is being used
                except:
                    print "Missing file size for " + aFile['file']
                if (currentTime - aFile['time']) < cutTime:                     # If the file is less than 2.5 weeks old, skip it
                    print 'Skipping the file ' + aDirectory + aFile['file'] + ' because it is new.'
                    continue
                found = False
                aName = aFile['file']
                for bBlock0 in bDirectoryList:
                    for bFile in bBlock0['files']:
                        if found:
                            break                                               # If file is found, then you can stop searching for it
                        if aFile['file'] == bFile['file']:                      # Here's the file comparison
                            found = True
                            break
                if not found:                                                   # If there was no match for the file, note in the report
                    if not wroteDataSetName:                                    # Can write the dataset name for matching directories
                        wroteDataSetName = True
                        writeBlock(bDirectoryList[0]['dataset'],report)         # Note this assumes all files in same directory are from same block
                    report.write(aDirectory + aName + ' \n')
                    try:
                        clearSize = clearSize + int(aFile['size'])              # Add to the space that would be cleared out
                    except:                                                     # If there's an error, it will be given in the final results
                        print aDirectory + aName + ' does not have a size'      # So there does not need to be a big deal here
        else:                                                                   # If entire directory is not matched, note this in report
            report.write('PhEDEx expects no files in ' + aDirectory + ' \n')    # This is the flag the files in directory should be removed
            for aFile in aBlock['files']:                                       # Find the space that would be cleared out for the whole directory
                try:
                    isUsed = isUsed + int(aFile['size'])                        # Add to the space that is being used
                    clearSize = clearSize + int(aFile['size'])                  # Add to the space that would be cleared out
                except:                                                         # If there's an error, it will be given in the final results
                    print aDirectory + aName + ' does not have a size'          # So there does not need to be a big deal here

    report.write('\n')
    # Stick some useful instructions at the end of the report
    report.write('****************************************************************************** \n')
    report.write('Space used in searched areas:         ' + str(float(isUsed)/2**30) + ' GB. \n')
    report.write('According to PhEDEx, should be using: ' + str(float(shouldBeSpace)/2**30) + ' GB. \n')
    report.write('****************************************************************************** \n')
    report.write('You are missing ' + str(float(missingSize)/2**30) + ' GB worth of files. \n')
    report.write('****************************************************************************** \n')
    report.write('If you run the following command:  \n')
    report.write('python ClearSite.py -T ' + TName + ' \n')
    report.write('You will clear ' + str(float(clearSize)/2**30) + ' GB of space. \n')
    report.write('****************************************************************************** \n')
    report.write('Run the following command to not pause for every directory:  \n\n')
    report.write('python ClearSite.py --fast -T ' + TName + ' \n\n')
    report.write('Run the following command to just give output of what will be deleted, \n')
    report.write('witout actually removing anything:  \n\n')
    report.write('python ClearSite.py --safe -T ' + TName + ' \n')
    report.write('              -- or --  \n')
    report.write('python ClearSite.py --safe --fast -T ' + TName + ' \n')
    report.close()
    if skipCksm:                                                               # Everything will be stored differently when skipping checksum calculations
        print 'Putting summary in: ' + TName + '_skipCksm_summary.txt'
        summary = open(TName + '_skipCksm_summary.txt','w')
    else:
        print 'Putting summary in: ' + TName + '_summary.txt'
        summary = open(TName + '_summary.txt','w')
    summary.write('Amount of space that can be cleared:           ' + str(float(clearSize)/2**30) + ' GB \n')
    summary.write('Estimated size of files that are missing:      ' + str(float(missingSize)/2**30) + ' GB \n')
    summary.write('Amount of space used in searched directories:  ' + str(float(isUsed)/2**30) + ' GB \n')
    summary.write('Amount of space PhEDEx expects to be using:    ' + str(float(shouldBeSpace)/2**30) + ' GB \n')
    if (clearSize == 0) and (missingSize == 0) and (isUsed != shouldBeSpace):
        summary.write('** Differences between space used and space expected to be used is caused by \n')
        summary.write('** inconsistencies in newer files or directories that were not searched. \n')
    localtime = time.strftime("%a %d %b %H:%M:%S",time.gmtime(time.time()))
    summary.write('This summary was generated at: ' + localtime + ' UTC \n')
    summary.close()
    return [missingSize,clearSize,shouldBeSpace,isUsed]
