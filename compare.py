import json, os

def finalCheck(TName,skipCksm):
    firstFile = open(TName + '_phedex.json')
    print 'Loading first file...'
    firstData = json.load(firstFile)
    firstFile.close()
    print 'Loaded...'

    if skipCksm:
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

    if skipCksm:
        print 'Report will be in: ' + TName + '_skipCksm_results.txt'
        report = open(TName + '_skipCksm_results.txt','w')
        report.write('Skipping Checksum (Alder32) comparisons! \n')
        print 'Started writing...'
    else:
        print 'Report will be in: ' + TName + '_results.txt'
        report = open(TName + '_results.txt','w')
    report.write('\nFiles missing at site: \n\n')
    for aBlock in firstData:
        foundDir = False
        aDirectory = aBlock['directory']
        for bBlock in secondData:
            if aBlock['directory'] == bBlock['directory']:
                foundDir = True
                wroteDataSetName = False
                for aFile in aBlock['files']:
                    found = False
                    aName = aFile['file']
                    aSize = aFile['size']
                    aCksm = aFile['adler32']
                    for bFile in bBlock['files']:

                        if aFile['file'] == bFile['file']:
                            found = True
                            if aFile['size'] == bFile['size'] and (skipCksm or aFile['adler32'] == bFile['adler32']):
                                break
                            else:
                                if not wroteDataSetName:
                                    wroteDataSetName = True
                                    report.write('------------------------------------------------------ \n')
                                    report.write('Dataset: ' + aBlock['dataset'] + ' \n')
                                    report.write('------------------------------------------------------ \n')
                                report.write(aDirectory + aName + ' has incorrect size or checksum: PhEDEx -- '+str(aCksm)+' '+str(aSize)+'; Site -- '+str(bCksm)+' '+str(bSize)+' \n')
                                break

                    if not found:
                        if not os.path.exists(aDirectory + aName):
                            if not wroteDataSetName:
                                wroteDataSetName = True
                                report.write('------------------------------------------------------ \n')
                                report.write('Dataset: ' + aBlock['dataset'] + ' \n')
                                report.write('------------------------------------------------------ \n')
                            report.write(aDirectory + aName + ' \n')
                        else:
                            if not wroteDataSetName:
                                wroteDataSetName = True
                                report.write('------------------------------------------------------ \n')
                                report.write('Dataset: ' + aBlock['dataset'] + ' \n')
                                report.write('------------------------------------------------------ \n')
                            report.write(aDirectory + aName + ' was not in a searched directory. \n')
        if not foundDir:
            report.write('------------------------------------------------------ \n')
            report.write('Dataset: ' + aBlock['dataset'] + ' \n')
            report.write('------------------------------------------------------ \n')
            report.write('No files were found in ' + aDirectory + ' \n')

    report.write('\n')
    report.write('File not in PhEDEx: \n\n')
    for aBlock in secondData:
        aDirectory = aBlock['directory']
        bDirectoryList = []
        for bBlock in firstData:
            if aBlock['directory'] == bBlock['directory']:
                bDirectoryList.append(bBlock)
        if len(bDirectoryList) > 0:
            for aFile in aBlock['files']:
                found = False
                aName = aFile['file']
                for bBlock0 in bDirectoryList:
                    for bFile in bBlock0['files']:
                        if aFile['file'] == bFile['file']:
                            found = True
                            break
                if not found:
                    report.write(aDirectory + aName + ' \n')
        else:
            report.write('PhEDEx expects nothing in ' + aDirectory + ' \n')
    report.write('\n')
    report.close()
