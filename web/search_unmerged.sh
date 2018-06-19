#! /bin/bash

site=$1
directory=$2

if [ -z $directory ]
then
    echo "USAGE: $0 SITE DIRECTORY"
    echo ""
    echo "Prints the time the last complete unmerged listing started for SITE"
    echo "and the files that were protected from deletion in DIRECTORY"
    exit 1
fi

download_file () {
    target=$1

    # If file does not exist, or it is more than 15 minutes old, try to download again
    if printf $target | perl -ne '! -f $_ || time - (stat $_)[9] > 900 || die' &> /dev/null
    then
        # Get current timestamp to avoid race conditions if modifying time
        now=$(date +'%Y%m%d%H%M.%S')

        # Only download if server's file is new
        wget -N http://dynamo.mit.edu/consistency/$target

        # Update modtime, even if not downloaded, to mark that it has been checked
        touch -t $now $target
    fi
}

download_file consistency_config.json

# Check that the directory asked for is not one of our ignored ones
if perl -e '
$/ = undef; my $fc = <>;                      # Read in config file as one string
$fc =~ /IgnoreDir.*?\[\s*"([^\]]*)/;          # Turn the IgnoreDirectories
my $expr = join("|", split(/[\s",]+/, $1));   #   into a regex
"'$directory'" =~ qr"$expr" || die' consistency_config.json &> /dev/null
then
    echo ""
    echo "Unfortunately, the directory you are asking for is one of the ones we ignore."
    echo "See 'IgnoreDirectories' list in the consistency_config.json file."
    echo ""
    exit 2
fi

# Name of the database file
dbfile=${site}_protected.db

download_file $dbfile

# Check that file exists and is non-zero size
if [ -s $dbfile ]
then
    echo ""
    echo "Timestamp: $(echo 'SELECT * FROM timestamp;' | sqlite3 $dbfile)"
    echo ""
    echo "Contents of $directory:"
    echo "SELECT file FROM files JOIN directories ON id=dir WHERE dirname='$directory';" | sqlite3 $dbfile
    echo ""
else
    # If exists, then zero size, so remove
    test -f $dbfile && rm $dbfile
    echo "Failed to download or locate database file."
    exit 3
fi
