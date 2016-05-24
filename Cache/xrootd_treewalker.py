#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Example to find all files in a given directory on a given site.
#---------------------------------------------------------------------------------------------------
import re,sys
import os,json,time,stat
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
    if len(sys.argv) == 1:   # This is just for testing
#        base_url = 'root://srm.unl.edu//'
#        dirs = [ '/store/test/xrootd/T2_US_Nebraska/store/data/Run2015E/JetHT/' ] # just testing

        base_url = 'root://xrootd.cmsaf.mit.edu//'
        dirs = [ '/store/test/xrootd/T2_US_MIT/store/mc/JobRobot/' ]

        for dir in dirs:

            filelist = process_dir(base_url,dir)
            print '\n =-=-=-= Listing: ' + dir
            print filelist

        exit(0)

    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument('--baseurl', '-b', metavar='BaseURL', dest='baseurl', 
                        default='root://' + str(os.environ.get('SE_' + os.environ.get('site')) + '//'),
                        help='Base URL to check with xrootd.')
    parser.add_argument(metavar='Dirs', dest='dirs', nargs='*', default=[], help='List of directories to look at in /store/')

    args = parser.parse_args()

    outputFileName = os.environ['fileBase'] + '_skipCksm_exists.json'

    if os.path.exists(outputFileName) and (int(time.time() - os.stat(outputFileName)[stat.ST_MTIME]) < int(os.environ.get('SiteDirListAge'))):
        print('Current directory listing is too young to die!')
        exit(0)

    output = []
    file_output = []
    directory = ''

    for dir in args.dirs:
        processed = process_dir(args.baseurl, '/store/' + dir + '/')

        print('Dumping output')
        print(processed)

        if len(processed[0]) == 0:
            print('No directories were searched.')
            exit(1)

        filelist = processed[0]

        for file in filelist:
            new_directory = os.environ.get('site_storeLoc') + '/store/' + dir + '/' + '/'.join(file[0].split('/')[:-2]) + '/'
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

    outfile = open(outputFileName,'w')
    outfile.write(json.dumps(output))
    outfile.close()
