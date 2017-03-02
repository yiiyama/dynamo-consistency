#! /bin/bash

# If we're using ReadTheDocs, then we want to make some fake modules for XRootD and htcondor

if [ "$READTHEDOCS" = "True" ] || [ "$TRAVIS" = "true" ]
then

    location=$(dirname "$0")
    echo "Making fake XRootD and common at $location/.."

    cp -r "$location"/docs/fakemodules/* "$location"/..

fi
