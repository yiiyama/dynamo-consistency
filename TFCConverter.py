import os,json
import deco

def CheckDir(tempPrefix,prefix):
    if os.path.isdir(tempPrefix+'/store'):                      # Check that this prefix directory exists
        if len(os.listdir(tempPrefix+'/store')) > 0:            # Check that it is not empty
            print 'Contents of Prefix + /store'
            print os.listdir(tempPrefix+'/store')
            if prefix != tempPrefix:
                if prefix != '':
                    print prefix + ' changing to ' + tempPrefix
                    print 'I hope that\'s right...'
                ##
                return True
    ##
    return False
                    
def GetPrefix(TName):
    tfcFile = open(TName + '_tfc.json')
    tfcData = json.load(tfcFile, object_hook = deco._decode_dict)       # This converts the unicode to ASCII strings (see deco.py)
    tfcFile.close()

    tfcPaths = []
    tfcNames = []

    print 'Converting LFN to PFN...'

    for check in tfcData['phedex']['storage-mapping']['array']:         # This is basically just checking that the TFC has an entry I understand
        print check
        if check['protocol'] == 'direct' and check['element_name'] == 'lfn-to-pfn':
            tfcPaths.append(check['result'])
            tfcNames.append(check['path-match'])
            print "tfcPaths:"
            print tfcPaths
            print "tfcNames:"
            print tfcNames
    ##
    prefix = ''
    for i0 in range(len(tfcNames)):
        tempPrefix = ''
        if len(tfcNames[i0].split('store')) > 1:                        # First, we check the case where the direct path-match is /+store/(.*) or /store/(.*)
            if tfcNames[i0].split('store')[1].split('(.*')[0] == '/':   # The latter happens in things that are not hadoop...
                tempPrefix = tfcPaths[i0].split('/store')[0]
                if CheckDir(tempPrefix,prefix):                         # Check if the directory seems appropriate
                    prefix = tempPrefix                                 # Set it then
            ##
            elif tfcNames[i0].split('(')[1].split('/.*')[0] == 'store':
                tempPrefix = tfcPaths[i0].split('$1')[0]
                if CheckDir(tempPrefix,prefix):                         # Check if the directory seems appropriate
                    prefix = tempPrefix                                 # Set it then
    ##
    if prefix == '':                                                    # If looking for store in the path-match was unsuccessful
        for i0 in range(len(tfcNames)):                                 # Look for the generic file path-match
            tempPrefix = ''
            if tfcNames[i0] == '/+(.*)' or tfcNames[i0] == '/(.*)':     # These are /+(.*) and perhaps /(.*), I think
                tempPrefix = tfcPaths[i0].split('/$1')[0]
                if CheckDir(tempPrefix):                                # Check if the directory seems appropriate
                    prefix = tempPrefix
    ##
    if prefix == '':
        print 'ERROR: Problem with the TFC.'                            # If not found yet, I give up
        exit()
    ##
    return prefix
