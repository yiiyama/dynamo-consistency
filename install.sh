#! /bin/bash

# If we're using ReadTheDocs, then we want to make some fake modules for XRootD and htcondor

location=$(dirname $(readlink -f "$0"))

if [ "$READTHEDOCS" = "True" ] || [ "$TRAVIS" = "true" ]
then

    echo "Making fake XRootD and common at $location/.."
    cp -r "$location"/docs/fakemodules/* "$location"/..

fi

# Backwards compatibility for now
ln -s "$location" "$location"/../dynamo_consistency
ln -s "$location" "$location"/../dynamo_consistency 
