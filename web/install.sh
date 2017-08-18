#! /bin/bash

TARGET=$1
DEFAULT=$HOME/public_html/ConsistencyCheck

if [ -z $TARGET ]
then

    TARGET=$DEFAULT
    echo "No target passed. Installing to default location: $TARGET"

elif [ "$TARGET" = "-h" -o "$TARGET" = "--help" ]
then

    echo ""
    echo "  Usage: $0 DIRECTORY"
    echo ""
    echo "  Installs webpage files into target directory."
    echo "  (Default: $DEFAULT)"
    echo ""
    exit 0

fi

test -d $TARGET || mkdir -p $TARGET

cp output.html stats.php stylin.css $TARGET

exit $?
