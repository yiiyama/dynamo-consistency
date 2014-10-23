import json, os

def finalCheck(TName,skipCksm):
    firstFile = open(TName + '_phedex.json')
    firstData = json.load(firstFile)
    firstFile.close()

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
    secondData = json.load(secondFile)
    secondFile.close()

    if skipCksm:
        print 'Report will be in: ' + TName + '_skipCksm_results.txt'
        report = open(TName + '_skipCksm_results.txt','w')
        report.write('Skipping Checksum (Alder32) comparisons! \n')
        print 'Started writing...'
    else:
        print 'Report will be in: ' + TName + '_results.txt'
        report = open(TName + '_results.txt','w')
    report.write('\nFile missing at site: \n\n')
    for aBlock in firstData:
        for aFile in aBlock:
            found = False
            aName = aFile['file']
            aSize = aFile['size']
            aCksm = aFile['adler32']
            for bBlock in secondData:
                for bFile in bBlock:
                    bName = bFile['file']
                    bSize = bFile['size']
                    bCksm = bFile['adler32']

                    if aName == bName:
                        found = True
                        if aSize == bSize and (skipCksm or aCksm == bCksm):
                            break
                        else:
                            report.write(aName + ' has incorrect size or checksum: PhEDEx -- '+str(aCksm)+' '+str(aSize)+'; Site -- '+str(bCksm)+' '+str(bSize)+' \n')
                            break
                if found:
                    break
            if not found:
                if not os.path.exists(aName):
                    report.write(aName + ' is missing from the site. \n')
                else:
                    report.write(aName + ' was not in a searched directory. \n')
    report.write('\n')
    report.write('File not in PhEDEx: \n\n')
    for aBlock in secondData:
        for aFile in aBlock:
            found = False
            aName = aFile['file']
            for bBlock in firstData:
                for bFile in bBlock:
                    bName = bFile['file']

                    if aName == bName:
                        found = True
                        break
                if found:
                    break
            if not found:
                report.write(aName + ' is not in PhEDEx database. \n')
    report.write('\n')
    report.close()
