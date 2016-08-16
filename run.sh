#!/bin/bash

nohup ./RunCheck.sh > $ConsistencyCacheDirectory/nohup_`date +%y%m%d`.out 2>&1&
