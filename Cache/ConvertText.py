#! /usr/bin/python

import os, sys, json, time

print('Trying to convert ' + os.environ['fileBase'] + '.txt')

def GetTime(uberDate):
    uberDate.append(str(time.gmtime(time.time()).tm_year))

    TryTime = time.mktime(time.strptime(' '.join(uberDate),'%b %d %H:%M %Y'))
    if TryTime > time.time():
        uberDate[-1] = str(int(uberDate[-1]) - 1)
        
    return time.mktime(time.strptime(' '.join(uberDate),'%b %d %H:%M %Y'))


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

uberOut.close()
        
if len(FileInDirList) != 0:
    OutputList.append({"directory": CurrentDirectory, "files": FileInDirList, "time": GetTime(DirectoryInfo[3:6])})
    FileInDirList = []

outfile = open(os.environ['fileBase'] + '_skipCksm_exists.json','w')
outfile.write(json.dumps(OutputList))
outfile.close()
