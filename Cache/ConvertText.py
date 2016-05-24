#! /usr/bin/python

import os, sys, json, time, re

print('Trying to convert ' + os.environ['fileBase'] + '.txt')

time_string = re.compile(r'[0-9][0-9]:[0-9][0-9]')

def GetTime(uberDate):
    uberSlice = []
    for index in range(len(uberDate)):
        if time_string.match(uberDate[index]):
            uberSlice = uberDate[index-2:index+1]
            break

    uberSlice.append(str(time.gmtime(time.time()).tm_year))

    TryTime = time.mktime(time.strptime(' '.join(uberSlice),'%b %d %H:%M %Y'))
    if TryTime > time.time():
        uberSlice[-1] = str(int(uberSlice[-1]) - 1)
        
    return time.mktime(time.strptime(' '.join(uberSlice),'%b %d %H:%M %Y'))


if not os.path.exists(os.environ['fileBase'] + '.txt'):
    print ('uberftp output seems to be missing.')
    print ('Check ' + os.environ['fileBase'] + '.txt')
    exit()

uberOut = open(os.environ['fileBase'] + '.txt','r')

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
            OutputList.append({"directory": CurrentDirectory, "files": FileInDirList, "time": GetTime(DirectoryInfo)})
            FileInDirList = []
        CurrentDirectory = NewDirectory

    FileInfo = line.strip('\n').split()
    FileFull = FileInfo[-1]
    FileSize = FileInfo[3]

    FileDirectory = '/'.join(FileFull.split('/')[:-4]) + '/'
    FileName = '/'.join(FileFull.split('/')[-4:])

    if FileDirectory == CurrentDirectory:
        FileInDirList.append({"time": GetTime(FileInfo), "adler32": "Not Checked", "file": FileName, "size": int(FileSize)})
    else:
        print ('Big problem in this thing, yo.')
        exit()

uberOut.close()
        
if len(FileInDirList) != 0:
    OutputList.append({"directory": CurrentDirectory, "files": FileInDirList, "time": GetTime(DirectoryInfo)})
    FileInDirList = []

outfile = open(os.environ['fileBase'] + '_skipCksm_exists.json','w')
outfile.write(json.dumps(OutputList))
outfile.close()
