#! /usr/bin/python

import os, sys, json, time

def GetTime(uberDate):
    uberDate.append(str(time.gmtime(time.time()).tm_year))

    TryTime = time.mktime(time.strptime(' '.join(uberDate),'%b %d %H:%M %Y'))
    if TryTime > time.time():
        uberDate[-1] = str(int(uberDate[-1]) - 1)
        
    return time.mktime(time.strptime(' '.join(uberDate),'%b %d %H:%M %Y'))


if len(sys.argv) < 2:
    print ('Give me the name of a site!')
    print (sys.argv[0] + ' <sitename>')
    exit()

siteName = sys.argv[1]
if not os.path.exists(siteName + '/' + siteName + '.txt'):
    print ('uberftp output seems to be missing.')
    print ('Check ' + siteName + '/' + siteName + '.txt')
    exit()


uberOut = open(siteName + '/' + siteName + '.txt','r')

OutputList = list()
DirectoryInfo = list()
NewDirectory = ''
CurrentDirectory = ''
FileInDirList = list()

for line in uberOut.readlines():
    if not line.endswith('.root\n'):
        DirectoryInfo = line.strip('\n').split()
        NewDirectory = '/'.join(DirectoryInfo[-1].split('/')[:-1]) + '/'
        continue

    if NewDirectory != CurrentDirectory:
        if len(FileInDirList) != 0:
            OutputList.append({"directory": CurrentDirectory, "files": FileInDirList, "time": GetTime(DirectoryInfo[3:6])})
            FileInDirList = []
        CurrentDirectory = NewDirectory

    FileInfo = line.strip('\n').split()
    FileFull = FileInfo[-1]
    FileSize = FileInfo[3]
    FileTime = FileInfo[4:7]      # This information doesn't seem to look complete coming out of the line

    FileDirectory = '/'.join(FileFull.split('/')[:-2]) + '/'
    FileName = '/'.join(FileFull.split('/')[-2:])

    if FileDirectory == CurrentDirectory:
        FileInDirList.append({"time": GetTime(FileTime), "adler32": "Not Checked", "file": FileName, "size": int(FileSize)})
    else:
        print ('Big problem in this thing, yo.')
        exit()
        
if len(FileInDirList) != 0:
    OutputList.append({"directory": CurrentDirectory, "files": FileInDirList, "time": GetTime(DirectoryInfo[3:6])})
    FileInDirList = []

uberOut.close()

outfile = open(siteName+'/'+siteName+'_skipCksm_exists.json','w')
outfile.write(json.dumps(OutputList))
outfile.close()
