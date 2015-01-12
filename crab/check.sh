#!/bin/bash

TASKNAME="$1"

crab -status    -c $TASKNAME
crab -getoutput -c $TASKNAME

exit 0
