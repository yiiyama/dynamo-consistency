#! /bin/bash

# Test that the standalone docs on readthedocs
# will compile correctly

# Run from root directory of the repository
test -f test/build_standalone_docs.sh || exit 2

# Install packages for sphinx first
pip install -r docs/requirements.txt

# Location of sphinx errors
output=test/standalone.err

sphinx-build -b html -E docs test/html 2> "$output"
cat "$output"

exitcode=0

# Search for problems
if grep -E "autodoc: failed to import|WARNING: undefined label:|contains reference to nonexisting document|ImportError" "$output"
then

    tput setaf 1 2> /dev/null
    echo "Problem in documentation build!"
    exitcode=1

fi

tput sgr0 2> /dev/null
exit "$errorcode"
