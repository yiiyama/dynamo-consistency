import os,json
import deco

def GetPrefix(TName):
    tfcFile = open(TName+'/'+TName+'_lfn2pfn.json')
    tfcData = json.load(tfcFile, object_hook = deco._decode_dict)            # This converts the unicode to ASCII strings (see deco.py)
    tfcFile.close()

    tfcPaths = []
    tfcNames = []

    prefix = ''
    for check in tfcData['phedex']['mapping']:                               # This is basically just checking that the TFC has an entry I understand
        lfnName = check['lfn']
        pfnName = check['pfn']

        prefix = pfnName.split(lfnName)[0]

    if prefix == '':
        print 'ERROR: Problem with the TFC.'                                 # If not found yet, I give up
        exit()
    ##
    return prefix
