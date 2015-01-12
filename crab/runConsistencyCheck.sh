#!/bin/bash
#---------------------------------------------------------------------------------------------------
# Execute one job (works interactively and when executed in condor)
#---------------------------------------------------------------------------------------------------
# find the line to this dataset and do further analysis
echo ""
echo " START run.sh"
echo ""
echo " -=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-";
echo " -=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-";
echo ""

echo "id:"
id
echo "hostname:"
hostname
echo "pwd:"
pwd
echo "ls -lhrt:"
ls -lhrt
echo "env:"
env

python ConsistencyCheck.py -c tempConfig.cfg

#
echo ""
echo " -=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-";
echo " -=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-.-=o=-";
echo " END run.sh"
echo ""
exit $?;
