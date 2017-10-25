#! /bin/bash

# Source dynamo
. $HOME/dynamo/etc/profile.d/init.sh

# Set PYTHONPATH
export PYTHONPATH=$(dirname $(dirname $HERE)):$HOME/dynamo/lib:$PYTHONPATH
