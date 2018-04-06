#! /bin/bash

# Source dynamo
. $HOME/dynamo/etc/profile.d/init.sh

HERE=$(cd $(dirname $BASH_SOURCE) && pwd)

# Set PYTHONPATH
export PYTHONPATH=$(dirname $(dirname $(dirname $HERE))):$HOME/dynamo/lib:$PYTHONPATH
