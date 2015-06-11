#! /bin/bash

TEST=$1

# This is a nice little script that submits jobs for me, cleans up my space, and updates the website

# This is where I am keeping all of the output:
STOREDIR='/scratch/dabercro/ConsistencyCheck/runs'
FAILDIR='/scratch/dabercro/ConsistencyCheck/FailedCheck'
SERVERDIR='/home/cmsprod/public_html/ConsistencyChecks'
CACHEDIR='/scratch/dabercro/ConsistencyCheck/Cache'

#declare -a SITES=("T1_US_FNAL_Disk")
#declare -a SITES=("T2_AT_Vienna" "T2_BE_IIHE" "T2_BE_UCL" "T2_BR_SPRACE" "T2_BR_UERJ" "T2_CH_CERN" "T2_CH_CSCS" "T2_CN_Beijing" "T2_DE_DESY" "T2_DE_RWTH" "T2_EE_Estonia" "T2_ES_CIEMAT" "T2_ES_IFCA" "T2_FI_HIP" "T2_FR_CCIN2P3" "T2_FR_GRIF_IRFU" "T2_FR_GRIF_LLR" "T2_FR_IPHC" "T2_GR_Ioannina" "T2_HU_Budapest" "T2_IN_TIFR" "T2_IT_Bari" "T2_IT_Legnaro" "T2_IT_Pisa" "T2_IT_Rome" "T2_KR_KNU" "T2_MY_UPM_BIRUNI" "T2_PK_NCP" "T2_PL_Swierk" "T2_PL_Warsaw" "T2_PT_NCG_Lisbon" "T2_RU_IHEP" "T2_RU_INR" "T2_RU_ITEP" "T2_RU_JINR" "T2_RU_PNPI" "T2_RU_RRC_KI" "T2_RU_SINP" "T2_TH_CUNSTDA" "T2_TR_METU" "T2_UA_KIPT" "T2_UK_London_Brunel" "T2_UK_London_IC" "T2_UK_SGrid_Bristol" "T2_UK_SGrid_RALPP" "T2_US_Caltech" "T2_US_Florida" "T2_US_MIT" "T2_US_Nebraska" "T2_US_Purdue" "T2_US_UCSD" "T2_US_Vanderbilt" "T2_US_Wisconsin")
declare -a SITES=("T2_AT_Vienna" "T2_BE_IIHE" "T2_BE_UCL" "T2_BR_UERJ" "T2_EE_Estonia" "T2_ES_CIEMAT" "T2_IT_Pisa" "T2_IT_Rome" "T2_US_Caltech" "T2_US_Florida" "T2_US_MIT" "T2_US_Nebraska" "T2_US_Purdue" "T2_US_UCSD" "T2_US_Vanderbilt" "T2_US_Wisconsin")
#declare -a SITES=("T2_US_MIT")

for SITE in "${SITES[@]}";do
    COUNTDIR=`ls -d $SITE-* | wc -l`
    if [ "$COUNTDIR" -eq "1" ]; then                             # Ideally, there will only be one directory per site
        DIRNAME=`ls -d $SITE-*`
        echo $DIRNAME
        DIRCHECK=`ls $DIRNAME/res | wc -l`                       # Check if any results have been posted
        if [ "$DIRCHECK" -eq "0" ]; then                         # If not, try to retrieve output
            ./check.sh $DIRNAME
        fi
        ISTAR=`ls $DIRNAME/res/$SITE.tar*.gz | wc -l`            # Look for the tarred output
        if [ "$ISTAR" -eq "1" ]; then                            # If it's there, update the website
            cd $DIRNAME/res                                      # cd into the .tar directory
            tar -xzvf $SITE.tar*.gz                              # Getting tar stuff out
            HASTEXT=`ls $SITE*.txt | wc -l`
            if [ "$HASTEXT" -gt "0" ]; then
                if [ ! -d "$SERVERDIR/$SITE" ]; then             # Make sure the right folder is there
                    echo "It looks like it's the first time for $SITE."
                    echo "Making a new folder!"                  # If not there, then make it
                    mkdir $SERVERDIR/$SITE
                fi
                cp $SITE*.txt $SERVERDIR/$SITE/.                 # Put results on the server
            else
                echo "Uh oh, no output..."
                rm $SITE*.json
                cp -r ../../$DIRNAME $FAILDIR/.                  # Store in a place specfically for debugging
            fi
            if [ ! -d "$CACHEDIR/$SITE" ]; then                  # Make sure the right folder is there
                echo "Preparing a cache for $SITE."
                echo "Making a new folder!"                      # If not there, then make it
                mkdir $CACHEDIR/$SITE
            fi
            cp $SITE.tar*.gz $CACHEDIR/$SITE/$SITE.tar.gz
            rm $SITE*.json $SITE*.txt                            # Clean up the stuff from tar
            cd -                                                 # Now cd back
            mv $SITE-* $STOREDIR/.                               # Now store that stuff
            COUNTDIR2=`ls -d $SITE-* | wc -l`                    # Check if it's still there, some log file causes this often
            if [ "$COUNTDIR2" -eq "1" ]; then                    # If it's there, remove it 
                rm -rf $DIRNAME
            fi
        elif [ "$ISTAR" -eq "0" ]; then                          # If there's no tar, check to see if the job finished
            if [ ! -f $DIRNAME/share/crab.cfg ]; then            # If there's no cfg file, then I tried to delete this before
                rm -rf $DIRNAME
            elif [ -f $DIRNAME/res/CMSSW_1.stdout ]; then        # If it did (with no tar) then there must have been an error
                echo "Looks like the job failed... Check that out later."
                cp -r $DIRNAME $STOREDIR/.                       # Store for long term
                mv $DIRNAME $FAILDIR/.                           # Also store in a place specfically for debugging
                COUNTDIR2=`ls -d $SITE-* | wc -l`                # Check if it's still there, some log file causes this often
                if [ "$COUNTDIR2" -eq "1" ]; then                # If it's there, remove it 
                    rm -rf $DIRNAME
                fi
            fi
        else
            echo "How did I get more tars??"                     # This would be a rather weird thing to happen
            echo "I'll just hide those and start over!"
            mv $DIRNAME $STOREDIR/.                              # But no need to throw it out
            COUNTDIR2=`ls -d $SITE-* | wc -l`                    # Check if it's still there, some log file causes this often
            if [ "$COUNTDIR2" -eq "1" ]; then                    # If it's there, remove it 
                rm -rf $DIRNAME
            fi
        fi
    else
        if [ "$COUNTDIR" -gt "1" ]; then                         # If more than one directory for a site
            echo "There are too many directories from $SITE!!"   # Something odd happened
            echo "I'll just hide those and start over!"          # I don't want to worry about that right now
            mv $SITE-* $STOREDIR/.
            COUNTDIR2=`ls -d $SITE-* | wc -l`                    # Check if it's still there, some log file causes this often
            if [ "$COUNTDIR2" -eq "1" ]; then                    # If it's there, remove it 
                rm -rf $DIRNAME
            fi
        fi
        echo 'Checking cache.'                                   # About to submit a job, so update the cache
        if [ "$TEST" == "s" ]; then                              # If specified
            cd $CACHEDIR                                         # Go to the cache storage place
#            ./updateCache.sh $SITE                               # Check and update the cache if necessary
            cd -                                                 # Come back    
        fi
        NOW=`date +"%Y-%m-%d-%T"`                                # Otherwise, there is no directory for a site
        echo ./submit.sh $SITE $NOW                              # It's easy to make a directory though
        if [ "$TEST" == "s" ]; then                              # If specified
            echo "Submitting..."
            ./submit.sh $SITE $NOW                               # Submits a job here
        fi
    fi
done

python updateList.py -D $SERVERDIR                               # Updates the site list used by the website
cd $SERVERDIR
chmod 644 */*.txt                                                # Allow all files to be read by people outside the server
cd -
