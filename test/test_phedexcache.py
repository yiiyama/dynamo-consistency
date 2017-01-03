"""
Jobs of phedexcache:

- Download lists of file replicas from Phedex
- Only do this if the cache is too old

- Return file replicas at a given site in the given format:

  Each directory contains subdirectories and files
  Each file has a size

  To speed up comparisons use the contents of a directory,
  along with file sizes, to generate a hash.
  (The top level directories will have hashes compared)

Things to test:

- Ensure that data downloaded from phedex have the expected format
- Check if duplicate calls to update the cache actually
  prevents update if called too soon
- Check that directories that are too new are not included
"""

if __name__ == '__main__':
    exit(0)
