#! /bin/bash

# Start by downloading the new files and deletions

#declare -a sites=("T2_AT_Vienna" "T2_BE_IIHE" "T2_BE_UCL" "T2_BR_SPRACE" "T2_BR_UERJ" "T2_CH_CERN" "T2_CH_CSCS" "T2_CN_Beijing" "T2_DE_DESY" "T2_DE_RWTH" "T2_EE_Estonia" "T2_ES_CIEMAT" "T2_ES_IFCA" "T2_FI_HIP" "T2_FR_CCIN2P3" "T2_FR_GRIF_IRFU" "T2_FR_GRIF_LLR" "T2_FR_IPHC" "T2_GR_Ioannina" "T2_HU_Budapest" "T2_IN_TIFR" "T2_IT_Bari" "T2_IT_Legnaro" "T2_IT_Pisa" "T2_IT_Rome" "T2_KR_KNU" "T2_MY_UPM_BIRUNI" "T2_PK_NCP" "T2_PL_Swierk" "T2_PL_Warsaw" "T2_PT_NCG_Lisbon" "T2_RU_IHEP" "T2_RU_INR" "T2_RU_ITEP" "T2_RU_JINR" "T2_RU_PNPI" "T2_RU_RRC_KI" "T2_RU_SINP" "T2_TH_CUNSTDA" "T2_TR_METU" "T2_UA_KIPT" "T2_UK_London_Brunel" "T2_UK_London_IC" "T2_UK_SGrid_Bristol" "T2_UK_SGrid_RALPP" "T2_US_Caltech" "T2_US_Florida" "T2_US_MIT" "T2_US_Nebraska" "T2_US_Purdue" "T2_US_UCSD" "T2_US_Vanderbilt" "T2_US_Wisconsin")
declare -a sites=("T2_US_MIT")

#for site in `ls -d T2_*/ | sed 's/[/].*$//'`; do
for site in $sites; do
    echo $site

    # First, let's generate a time that we are interested in looking at
#    origtime=`/home/dabercro/./jq -M .phedex.request_timestamp $site/$site\_added.json | sed 's/\..*$//'`
    origtime=`/home/dabercro/./jq -M .phedex.request_timestamp $site/$site.json | sed 's/\..*$//'`
    now=`date +%s`
    oldtime=`expr $now - 302400`                    # Anything older than half a week, time to download
    if [ $oldtime -gt $origtime ]; then
        requesttime=`expr $origtime - 6048000`       # Request data one week older than the previous request
        requestdelete=`expr $origtime - 907200`     # Request deletions 1.5 weeks older than the previous request
        echo Requesting time: $requesttime

        wget --no-check-certificate -O $site/$site\_added.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node=$site\&update_since=$requesttime
        wget --no-check-certificate -O $site/$site\_deleted.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/deletions?dataset=/*/*/*\&node=$site\&request_since=$requestdelete

        # Just temporary until I get this junk working...
        /home/dabercro/./jq -M '.phedex|[.block[]|{directory:.file[0].name|split("/")[0:-1]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-1:][0],size:.bytes}],dataset:.name}]}' $site/$site.json > $site/$site\_prephedex.json

        # Format the data
        /home/dabercro/./jq -M '.phedex|{request_time:.request_timestamp,block:[.dataset[].block[0]|{name,time:.deletion[0].time_complete}]}' $site/$site\_deleted.json > $site/$site\_formatted_deleted.json
        /home/dabercro/./jq -M '.phedex|{request_timestamp,block:[.block[]|{directory:.file[0].name|split("/")[0:-1]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-1:][0],size:.bytes}],dataset:.name}]}' $site/$site\_added.json > $site/$site\_formatted_added.json
        /home/dabercro/./jq -M -s '.[0] + .[1].block' $site/$site\_phedex.json $site/$site\_formatted_added.json > $site/$site\_temp.json
        cp $site/$site\_temp.json $site/$site\_prephedex.json
        python mergeFiles.py -T $site
    else
        echo $oldtime' ; '$origtime
    fi
done
