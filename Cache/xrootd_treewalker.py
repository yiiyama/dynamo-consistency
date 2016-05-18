#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Example to find all files in a given directory on a given site.
#---------------------------------------------------------------------------------------------------
import re,sys
import json
import XRootD.client

user_dir_regexp = re.compile(r'^/+user/+([A-Za-z0-9.]+)/*$')

def process_dir(base_url, directory):
    # process a given directory using a xrootd access point

    fs = XRootD.client.FileSystem(base_url)

    worklist = [directory]
    filelist = []
    failed_list = []
    base_len = len(directory)

    while worklist:
        cwd = worklist.pop()

        if user_dir_regexp.match(cwd):
            cwd += "/public"

        # remove duplicated '/' for cleansiness
        cwd = re.sub('/+','/',cwd)

        # say which directory we are probing
        print "Processing", cwd
        status, dirlist = fs.dirlist("/" + cwd,flags=XRootD.client.flags.DirListFlags.STAT)

        if status.status:
            print "Failed to list directory:", cwd
            failed_list.append("/" + cwd)
            continue

        for entry in dirlist.dirlist:
            if entry.statinfo.flags & XRootD.client.flags.StatInfoFlags.IS_DIR:
                worklist.append(cwd + "/" + entry.name)
            else:
                fname = cwd + entry.name
                filelist.append((fname[base_len:], entry.statinfo.size, entry.statinfo.modtime))

    return filelist, failed_list

def get_times(file_list):
    times = []
    for file in file_list:
        times.append(file['time'])

    return times

#--------------------------------------------------------------------------------------------------
#  M A I N
#--------------------------------------------------------------------------------------------------

# LAST CHARACTER IN THE DIR HAS TO BE A   '/'

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument('--sitename', '-s', metavar='SiteName', dest='sitename', default=os.environ.get('site'),
                        help='Name of site to run treewalker on.')
    parser.add_argument('--baseurl', '-b', metavar='BaseURL', dest='baseurl', default=os.environ.get('SE_' + os.environ.get('site')),
                        help='Base URL to check with xrootd.')
    parser.add_argument(metavar='Dirs', dest='dirs', nargs='*', default=[], help='List of directories to look at in /store/')

    args = parser.parse_args()

    output = []
    file_output = []
    directory = ''

    for dir in args.dirs:
        processed = process_dir(args.baseurl, '/store/' + dir + '/')

        filelist = processed[0]

        for file in filelist:
            new_directory = os.environ.get('site_storeLoc') + '/' + dir + '/' + '/'.join(file[0].split('/')[:-2]) + '/'
            if new_directory != directory:
                if directory != '':
                    output.append({
                            "directory": directory,
                            "files": file_output,
                            "time": max(get_times(file_output))
                            })

                directory = new_directory
                file_output = []

            file_output.append({
                    "time": file[2],
                    "adler32": "Not Checked",
                    "file": '/'.join(file[0].split('/')[-2:]),
                    "size": file[1]
                    })

    output.append({
            "directory": directory,
            "files": file_output,
            "time": max(get_times(file_output))
            })

    outfile = open(os.environ['fileBase'] + '_skipCksm_exists.json','w')
    outfile.write(json.dumps(output))
    outfile.close()

else:
    base_url = 'root://srm.unl.edu//'
    dirs = [ '/store/test/xrootd/T2_US_Nebraska/store/data/Run2015E/JetHT/' ] # just testing
    # [ '/store/test/xrootd/T2_US_Nebraska/store/data/', '/store/test/xrootd/T2_US_Nebraska/store/mc/' ]

    for dir in dirs:

        filelist = process_dir(base_url,dir)
        print '\n =-=-=-= Listing: ' + dir
        print filelist
