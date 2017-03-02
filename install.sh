#! /bin/bash

# If we're using ReadTheDocs, then we want to make some fake modules for XRootD and htcondor

if [ "$READTHEDOCS" = "True" ] || [ "$TRAVIS" = "true" ]
then

    location=$(basname $0)
    echo "Making fake XRootD and htcondor at $location"

    mv $location/docs/fakemodules/* $location

fi
