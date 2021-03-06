#! /bin/bash

NUMBER=$1
MATCH=$2
FLAG=$3

if [ -z "$MATCH" -o "$NUMBER" = "-h" -o "$NUMBER" = "--help" ]
then
    perldoc -T $0
    exit 0
fi

if [ -z "$FLAG" ]
then
    BADSELECT=''
else
    BADSELECT='AND (stats.missing > 1000 OR stats.orphan > 1000 OR stats.files = 0)'
fi

# Make sure we have enough memory free or cached (6 GBi)
test $(perl -nae 'if (/^(MemFree|Cached):/) { $sum += $F[1] } } END { print "$sum"' /proc/meminfo) -gt 20000000 || exit 0

# Add jq to the system path
PATH=$PATH:/home/dabercro/bin

# Get the directory of this script (should be in prod)
HERE=$(cd $(dirname $0) && pwd)

# Setup environment
# (sources dynamo and sets PYTHONPATH
. $HERE/set_env.sh

# Determine the SQLite3 database location from the configuration file
DATABASE="-init $HERE/init.sql $(jq -r '.WebDir' $HERE/consistency_config.json)/stats.db"

# Don't know why it would happen, but protect against simple SQL injection
case $NUMBER in 
    *';'* ) exit 1 ;;
esac
case $MATCH in 
    *["'"';']* ) exit 1 ;;
esac

# Get the possible sites that match the constraint from dynamo
# Make sure users can only run over T2s or T1 disks
ALL_SITES=$(echo "SELECT name FROM sites WHERE name LIKE '$MATCH' AND (name LIKE 'T2_%' OR name LIKE 'T1_%_Disk');" | mysql --defaults-group-suffix=-dynamo -Ddynamo --skip-column-names)

# Make sure all of the sites are in the webpage's database
# Shove it all into one pipe to avoid too many connections
# Basically just looping over sites and inserting them
echo $ALL_SITES | tr ' ' '\n' | xargs -n1 -I '{SITE}' echo "INSERT OR IGNORE INTO sites VALUES ('{SITE}', 0, 0, NULL);" | sqlite3 $DATABASE

# Disable bad sites
BAD_SITES=$(echo "SELECT name FROM sites WHERE status != 'ready';" | mysql --defaults-group-suffix=-dynamo -Ddynamo --skip-column-names)
echo "UPDATE sites SET isrunning = -1 WHERE (site = '$(echo $BAD_SITES | sed "s/ /' OR site = '/g")') AND isrunning = 0;" | sqlite3 $DATABASE

# Enable now good sites
GOOD_SITES=$(echo "SELECT name FROM sites WHERE status = 'ready';" | mysql --defaults-group-suffix=-dynamo -Ddynamo --skip-column-names)
echo "UPDATE sites SET isrunning = 0 WHERE (site = '$(echo $GOOD_SITES | sed "s/ /' OR site = '/g")') AND isrunning = -1;" | sqlite3 $DATABASE

# Check SAM tests
./check_sam.py $DATABASE $GOOD_SITES >& /dev/null

# Now get a list of sites to run on
SITES=$(echo "
SELECT sites.site FROM sites 
LEFT JOIN stats ON sites.site=stats.site
WHERE isrunning = 0
AND (sites.site = '$(echo $ALL_SITES | sed "s/ /' OR sites.site = '/g")')
$BADSELECT
ORDER BY stats.entered ASC
LIMIT $NUMBER;
" | sqlite3 $DATABASE)

# Check machine
if [ "$USER" != "dynamo" -o `hostname` = 't3serv017.mit.edu' ]
then

    # Some additional setup
    export X509_USER_PROXY=/tmp/x509up_u$(id -u)

    if [ "$USER" != "dynamo" ]
    then

        # If dynamo, keeping this fresh is notmyjob
        voms-proxy-info -e || voms-proxy-init -voms cms --valid 192:00

    fi

    # Lock all sites first
    echo "UPDATE sites SET isrunning = 1 WHERE site = '$(echo $SITES | sed "s/ /' OR site = '/g")';" | sqlite3 $DATABASE

    # Run over each site
    for SITE in $SITES
    do

        # Get the location of the log files
        LOGLOCATION=$(jq -r '.LogLocation' $HERE/consistency_config.json)
        test -d $LOGLOCATION || mkdir -p $LOGLOCATION

        jq '.AccessMethod' consistency_config.json | grep SRM | grep $SITE >& /dev/null
        ISSRM=$?

        # Get SRM key for lock, if needed
        if [ $ISSRM = 0 ]
        then
            while ! rm gfal.key 2> /dev/null
            do
                sleep 300
            done
        fi

        # Report start of run
        echo "UPDATE sites SET laststarted = DATETIME('$(date +%Y-%m-%d\ %H:%M:%S)') WHERE site = '$SITE';" | sqlite3 $DATABASE
        echo "$(date) Starting run on $SITE" >> $LOGLOCATION/run_checks.log

        # Report running
        echo "UPDATE sites SET isrunning = 2 WHERE site = '$SITE';" | sqlite3 $DATABASE

        LOGFILE=${SITE}_$(date +%y%m%d_%H%M%S).log

        # Run
        $HERE/compare.py $SITE watch &> $LOGLOCATION/$LOGFILE

        # Copy log file to web location
        cp $LOGLOCATION/$LOGFILE $(jq -r '.WebDir' $HERE/consistency_config.json)/${SITE}.log
        # Parse for unlisted directories
        pushd $(jq -r '.WebDir' $HERE/consistency_config.json) >& /dev/null
        perl -ne '/ERROR.*Giving\sup\sdirectory\s([\w\/\-]+)/ && print "$1\n"' ${SITE}.log > ${SITE}_unlisted.txt
        popd >& /dev/null

        # Put key back
        if [ $ISSRM = 0 ]
        then
            touch gfal.key
        fi
        # Unlock in database
        echo "UPDATE sites SET isrunning = 0 WHERE site = '$SITE';" | sqlite3 $DATABASE

        echo "$(date) Finished run on $SITE" >> $LOGLOCATION/run_checks.log

    done

else

    echo "You're on the wrong machine, but everything else worked fine for: $SITES"

fi


exit 0

: <<EOF

=pod

=head1 Usage:

   run_checks.sh <MAXNUMBER> <MATCH> [<FLAG>]

runs the Consistency Check for sites that match the name MATCH
(using a MySQL "LIKE" expression), limited to MAXNUMBER.
Sites that have not been run before will get priority.
After that, priority is assigned by the sites that have gone the longest
without getting a new summary entry in the summary webpage.
Sites that are currently running are excluded.

If any value for FLAG is given, only sites with no files listed or
more than 1000 missing or orphan files will be considered.

=head1 Examples:

   run_checks.sh 1 T2_US_MIT                           # If you want to run on a single site
   ListAge=0 InventoryAge=0 run_checks.sh 1 T2_US_MIT  # To get a fresh cache, using environment variables to override configuration
   run_checks.sh 10 T2_%                               # Run on 10 high priority tier-2 sites
   run_checks.sh 2 T1_%_Disk                           # Start 2 tier-1 sites

=head1 Author

Daniel Abercrmbie <dabercro@mit.edu>

=cut

EOF
