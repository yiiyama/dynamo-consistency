import json
import deco

def GetPrefix(TName):
    tfcFile = open(TName + '_tfc.json')
    tfcData = json.load(tfcFile, object_hook = deco._decode_dict)       # This converts the unicode to ASCII strings (see deco.py)
    tfcFile.close()

    tfcPath = ''
    tfcName = ''

    print 'Converting LFN to PFN...'

    for check in tfcData['phedex']['storage-mapping']['array']:         # This is basically just checking that the TFC has an entry I understand
        print check
        if check['protocol'] == 'direct' and check['element_name'] == 'lfn-to-pfn':
            tfcPath = check['result']
            tfcName = check['path-match']
            print "tfcPath:"
            print tfcPath
            print "tfcName:"
            print tfcName

    if tfcPath.split('$')[-1] == '1':                                   # If the format matches, it'll have a /somestuff/$1 at the end
        remove = tfcName.split('+')[-1].split('(.*)')[0]                # which I can just take off and add to the front of the LFN
        if(len(remove) > 0):
            preFix = tfcPath.split('/'+remove+'$')[0:-1]
        else:
            preFix = tfcPath.split('/$')[0:-1]
        print 'Looks good...'
        print preFix[0]
    else:
        print 'ERROR: Problem with the TFC.'                            # If the format is unexpected, I give up
        exit()

    return [preFix[0],preFix[0]+'/store']
