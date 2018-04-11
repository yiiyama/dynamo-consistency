#! /bin/bash

# Source dynamo
. $HOME/dynamo/etc/profile.d/init.sh

HERE=$(cd $(dirname $BASH_SOURCE) && pwd)
opsspace=$(dirname $(dirname $HERE))

# Set PYTHONPATH
export PYTHONPATH=$opsspace:$opsspace/SiteAdminToolkit/unmerged-cleaner:$HOME/dynamo/lib:$PYTHONPATH:$HERE
