#!/bin/bash

source Config/ConsistencyConfig.sh

mkdir -p $ConsistencyCacheDirectory

nohup ./RunCheck.sh > $ConsistencyCacheDirectory/nohup_`date +%y%m%d`.out 2>&1&
