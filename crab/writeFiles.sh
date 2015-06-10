#! /bin/bash

SITENAME="$1"

crabConfFile="crab.cfg"

echo "[CRAB]"                                              > $crabConfFile
echo "jobtype                   = cmssw"                  >> $crabConfFile
echo "scheduler                 = remoteGlidein"          >> $crabConfFile
echo ""                                                   >> $crabConfFile
echo "[CMSSW]"                                            >> $crabConfFile
echo "datasetpath               = None"                   >> $crabConfFile
echo "pset                      = cmssw.py"               >> $crabConfFile
echo "total_number_of_events    = 100000"                 >> $crabConfFile
echo "output_file               = "$SITENAME".tar.gz"     >> $crabConfFile
echo "number_of_jobs            = 1"                      >> $crabConfFile
echo ""                                                   >> $crabConfFile
echo "[USER]"                                             >> $crabConfFile
echo "script_exe                = runConsistencyCheck.sh" >> $crabConfFile
echo "return_data               = 1"                      >> $crabConfFile
echo "copy_data                 = 0"                      >> $crabConfFile
echo ""                                                   >> $crabConfFile
echo "additional_input_files    = ../ConsistencyCheck/ConsistencyCheck.py,../ConsistencyCheck/LFN2PFNConverter.py,../ConsistencyCheck/compare.py,../ConsistencyCheck/deco.py,tempConfig.cfg,/scratch/dabercro/ConsistencyCheck/Cache/"$SITENAME"/"$SITENAME"_phedex.json,/scratch/dabercro/ConsistencyCheck/Cache/"$SITENAME"/"$SITENAME"_lfn2pfn.json" >> $crabConfFile
echo ""                                                   >> $crabConfFile
echo "[GRID]"                                             >> $crabConfFile
echo "rb                        = CERN"                   >> $crabConfFile
echo "maxtarballsize            = 3000"                   >> $crabConfFile

ConsistencyConfigFile="tempConfig.cfg"

echo "[General]"                    > $ConsistencyConfigFile
echo "# Name of the computing site being checked; Needed to download file list from PhEDEx"                                      >> $ConsistencyConfigFile
echo "SiteName = "$SITENAME        >> $ConsistencyConfigFile
echo ""                            >> $ConsistencyConfigFile
echo "########################################################################################################################"  >> $ConsistencyConfigFile
echo ""                            >> $ConsistencyConfigFile
cat ConsistencyConfig.cfg          >> $ConsistencyConfigFile
