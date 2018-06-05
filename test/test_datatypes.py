#! /usr/bin/env python

"""
Jobs of datatypes:

- Define a tree type that can be quickly compared with other trees
- Save the tree without data loss
- Remote listing and using a local list to check consistency
- In the tree, files that are too new should not affect the comparison

Things to test:

x Save and load trees and compare them to trees only in memory
x Creation of tree through list of files and through a filler function
  and compare them to see if they're the same
x Create two different trees and make sure the differences noted are correct
x Create new files and see if they affect the hash
x Create multiple trees and merge them
x Identifies empty directories to be removed
x Test size to clear
x Ignores directories that are too new

This is the development test script. Please don't touch if you're not me.
"""

import os
import sys
import time
import shutil
import unittest
import logging

from dynamo_consistency import datatypes

TMP_DIR = 'TempConsistency'
LOG = logging.getLogger(__name__)

# Define a filler function to use in the "remote filling" test
def my_ls(path, location=TMP_DIR):

    full_path = os.path.join(location, path)
    results = [os.path.join(full_path, res) for res in os.listdir(full_path)]

    dirs  = [(os.path.basename(name), os.stat(name).st_mtime) for \
                 name in filter(os.path.isdir, results)]
    files = [(os.path.basename(name), os.stat(name).st_size, os.stat(name).st_mtime) for \
                 name in filter(os.path.isfile, results)]

    return True, dirs, files

class TestBase(unittest.TestCase):

    file_list = [
        ('/store/mc/ttThings/0000/qwert.root', 20),
        ('/store/mc/ttThings/0000/qwery.root', 30),
        ('/store/mc/ttThings/0001/zxcvb.root', 50),
        ('/store/mc/ttThings/0000/doulb.root', 30),
        ('/store/mc/ttThings/00000/extra_zero.root', 30),
        ('/store/data/runB/earlyfile.root', 5),
        ('/store/data/runB/0001/missi.root', 45),
        ('/store/data/runA/0030/stuff.root', 10),
        ]

    def setUp(self):
        if os.path.exists(TMP_DIR):
            print 'Desired directory location already exists!'
            exit(1)
        os.makedirs(TMP_DIR)
        self.tree = datatypes.DirectoryInfo('/store')
        self.tree.add_file_list(self.file_list)
        self.do_more_setup()

    def tearDown(self):
        if os.path.exists(TMP_DIR):
            shutil.rmtree(TMP_DIR)

    def do_more_setup(self):
        pass

    def check_equal(self, tree0, tree1):

        tree0.setup_hash()
        tree1.setup_hash()

        self.assertEqual(tree0.hash, tree1.hash,
                         '%s\n=\n%s' % (tree0.displays(), tree1.displays()))
        self.assertEqual([fi['hash'] for fi in tree0._grab_first().files],
                         [fi['hash'] for fi in tree1._grab_first().files])
        self.assertEqual(tree0.get_num_files(), tree1.get_num_files())
        self.assertEqual(tree0.get_num_files(True), tree1.get_num_files(True))

class TestTree(TestBase):

    def test_directory_size(self):
        test_dir = lambda x: self.assertEqual(
            self.tree.get_node(x).get_directory_size(),
            sum([size for name, size in self.file_list if '%s/' % x in name]))

        # Test /store
        test_dir('')
        test_dir('mc')
        test_dir('data')
        test_dir('mc/ttThings')
        test_dir('mc/ttThings/0000')
        test_dir('data')
        test_dir('data/runB')

        # These now throw errors because the files are None
        self.assertRaises(TypeError, test_dir, 'fake')
        self.assertRaises(TypeError, test_dir, 'fake/directory/name')

    def test_num_files(self):
        self.assertEqual(self.tree.get_num_files(),
                         len(self.file_list))
        self.assertEqual(self.tree.get_num_files(True), 0)

    def test_two_lists(self):
        self.tree.add_file_list(self.file_list)
        self.assertEqual(self.tree.get_num_files(),
                         len(self.file_list))

    def test_do_hash(self):
        self.assertFalse(self.tree.hash)
        self.tree.setup_hash()
        self.assertTrue(self.tree.hash)

    def test_compare_saved(self):
        self.tree.save(os.path.join(TMP_DIR, 'tree.pkl'))
        tree0 = datatypes.get_info(os.path.join(TMP_DIR, 'tree.pkl'))

        self.check_equal(self.tree, tree0)

    def test_empty_compare(self):
        self.tree.setup_hash()
        file_list, dir_list, size = self.tree.compare(None)

        self.assertEqual(len(file_list), len(self.file_list))
        self.assertEqual(len(dir_list), 0)
        self.assertEqual(size, sum([size for _, size in self.file_list]))

    def test_merge_trees(self):
        trees = {
            'mc': datatypes.DirectoryInfo('mc'),
            'data': datatypes.DirectoryInfo('data')
            }

        for key, tree in trees.iteritems():
            tree.add_file_list([('/'.join(name.split('/')[2:]), size) \
                                    for name, size in self.file_list if name.split('/')[2] == key])

        one_tree = datatypes.DirectoryInfo('/store', [trees['data'], trees['mc']])
        self.check_equal(self.tree, one_tree)

        # Hopefully order doesn't matter

        two_tree = datatypes.DirectoryInfo('/store', [trees['mc'], trees['data']])
        self.check_equal(self.tree, two_tree)
        self.check_equal(one_tree, two_tree)

class TestConsistentTrees(TestBase):

    def do_more_setup(self):
        for name, size in self.file_list:
            path = os.path.join(TMP_DIR, name[7:])
            if not os.path.isdir(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            out = open(path, 'w')
            out.write('\0' * size)
            out.close()

            os.utime(path, (1000000000, 1000000000))

    def test_callback(self):
        called = {'check': False}
        def call(tree):
            LOG.info('In callback: tree has %i files', tree.get_num_files())
            called['check'] = True

        datatypes.create_dirinfo('', 'mc', my_ls, callback=call)
        self.assertTrue(called['check'])

    def test_ls_vs_list(self):

        dirinfos = [datatypes.create_dirinfo('', subdir, my_ls) \
                        for subdir in ['mc', 'data']]

        master_dirinfo = datatypes.DirectoryInfo('/store', directories=dirinfos)

        self.tree.display()
        master_dirinfo.display()

        self.check_equal(self.tree, master_dirinfo)
        self.assertEqual(self.tree.count_nodes(), master_dirinfo.count_nodes())

    def test_newdir(self):
        empty_dir = 'mc/new/empty/0002'

        os.makedirs(os.path.join(TMP_DIR, empty_dir))

        dirinfos = [datatypes.create_dirinfo('', subdir, my_ls) \
                        for subdir in ['mc', 'data']]

        master_dirinfo = datatypes.DirectoryInfo('/store', directories=dirinfos)

        self.check_equal(self.tree, master_dirinfo)
        self.assertFalse(master_dirinfo.get_node(os.path.join(empty_dir, 'not_there'), False))
        self.assertTrue(master_dirinfo.get_node(empty_dir, False))
        self.assertFalse(master_dirinfo.get_node(empty_dir, False).can_compare)


class TestInconsistentTrees(TestBase):

    listing = None

    orphan = [
        ('/store/data/runE/0000/toomany.root', 20)
        ]
    missing = [
        ('/store/mc/Zllll/0023/signal.root', 15)
        ]
    new_file = [
        ('/store/data/runQ/0000/recent.root', 10)
        ]

    def do_more_setup(self):
        for name, size in self.file_list + self.orphan:
            path = os.path.join(TMP_DIR, name[7:])
            if not os.path.isdir(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            out = open(path, 'w')
            out.write('\0' * size)
            out.close()

            os.utime(path, (1000000000, 1000000000))

        self.tree.add_file_list(self.missing)

        for name, size in self.new_file:
            path = os.path.join(TMP_DIR, name[7:])
            if not os.path.isdir(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            out = open(path, 'w')
            out.write('\0' * size)
            out.close()

        self.listing = datatypes.DirectoryInfo(
            '/store', directories=[datatypes.create_dirinfo('', subdir, my_ls)
                                   for subdir in ['mc', 'data']])

        self.tree.setup_hash()
        self.listing.setup_hash()

    def test_orphan(self):
        file_list, dir_list, _ = self.listing.compare(self.tree)

        self.assertEqual(len(file_list), 1)
        self.assertEqual(file_list[0], self.orphan[0][0])

    def test_missing(self):
        file_list, dir_list, _ = self.tree.compare(self.listing)

        self.assertEqual(len(file_list), 1)
        self.assertEqual(file_list[0], self.missing[0][0])

    def test_both(self):
        file_base = os.path.join(TMP_DIR, 'report')
        datatypes.compare(self.tree, self.listing, file_base)

        with open('%s_missing.txt' % file_base, 'r') as missing_file:
            missing = missing_file.readlines()

        with open('%s_orphan.txt' % file_base, 'r') as orphan_file:
            orphan = orphan_file.readlines()

        self.assertEqual(len(missing), 1)
        self.assertEqual(len(orphan), 1)

        self.assertEqual(missing[0].strip(), self.missing[0][0])
        self.assertEqual(orphan[0].strip(), self.orphan[0][0])

    def test_olddir(self):
        empty_dir = 'mc/new/empty/0002'
        path = os.path.join(TMP_DIR, empty_dir)
        os.makedirs(path)
        os.utime(path, (1000000000, 1000000000))

        listing = datatypes.DirectoryInfo('/store',
                                          directories=[datatypes.create_dirinfo('', subdir, my_ls)
                                                       for subdir in ['mc', 'data']])

        LOG.info('='*40)
        LOG.info('Doing the hash')
        LOG.info('='*40)

        listing.setup_hash()

        LOG.info('='*40)
        LOG.info('Doing the comparison')
        LOG.info('='*40)

        LOG.info(listing.displays())
        LOG.info('='*40)
        LOG.info(self.listing.displays())

        file_list, dir_list, _ = listing.compare(self.listing)

        self.assertEqual(len(dir_list), 1)
        self.assertTrue(os.path.join('/store', empty_dir).startswith(dir_list[0]))

        for file_name, _ in self.file_list:
            self.assertFalse(file_name.startswith(dir_list[0]))

    def test_new_file(self):
        self.tree.add_file_list(self.orphan)
        self.listing.add_file_list(self.missing)

        self.assertNotEqual(self.tree.get_num_files(), self.listing.get_num_files())
        self.assertEqual(self.tree.get_num_files() + len(self.new_file),
                         self.listing.get_num_files())

        self.tree.setup_hash()
        self.listing.setup_hash()

        self.assertEqual(self.tree.hash, self.listing.hash,
                         '%s\n=\n%s' % (self.tree.displays(), self.listing.displays()))

    def test_same_dir(self):
        self.tree.add_file_list(self.orphan)
        self.listing.add_file_list(self.missing)

        same_dir = [('/store/data/runB/0001/misso.root', 35)]

        self.tree.add_file_list(same_dir)

        self.listing.setup_hash()
        self.tree.setup_hash()

        LOG.info('=' * 30)
        LOG.info('Building done, going to compare')
        LOG.info('=' * 30)

        file_list, dir_list, _ = self.tree.compare(self.listing)

        self.assertFalse(dir_list)
        self.assertTrue(file_list)
        self.assertEqual(len(file_list), 1)
        self.assertEqual(file_list[0], same_dir[0][0])

    def test_double_check(self):
        file_base = os.path.join(TMP_DIR, 'report')

        check_true = lambda dummy: True
        check_false = lambda dummy: False
        check_miss = lambda x: x in [y[0] for y in self.missing]
        check_orph = lambda x: x in [y[0] for y in self.orphan]

        # Remove orphan
        for orphan_check, missing_check in [
            (check_true, None),
            (check_true, check_false),
            (check_orph, check_false),
            (check_orph, check_orph)
            ]:

            missing, m_size, orphan, o_size = datatypes.compare(
                self.tree, self.listing, file_base, orphan_check, missing_check)

            self.assertEqual(len(missing), 1)
            self.assertTrue(m_size)
            self.assertEqual(len(orphan), 0)
            self.assertFalse(o_size)

        # Remove missing
        for orphan_check, missing_check in [
            (None, check_true),
            (check_false, check_true),
            (check_false, check_miss),
            (check_miss, check_miss)
            ]:

            missing, m_size, orphan, o_size = datatypes.compare(
                self.tree, self.listing, file_base, orphan_check, missing_check)

            self.assertEqual(len(missing), 0)
            self.assertFalse(m_size)
            self.assertEqual(len(orphan), 1)
            self.assertTrue(o_size)

        # Remove both
        for orphan_check, missing_check in [
            (check_true, check_true),
            (check_orph, check_miss)
            ]:

            missing, m_size, orphan, o_size = datatypes.compare(
                self.tree, self.listing, file_base, orphan_check, missing_check)

            self.assertEqual(len(missing), 0)
            self.assertFalse(m_size)
            self.assertEqual(len(orphan), 0)
            self.assertFalse(o_size)

        # Remove neither
        for orphan_check, missing_check in [
            (None, None),
            (check_false, check_false),
            (check_miss, check_orph)
            ]:

            missing, m_size, orphan, o_size = datatypes.compare(
                self.tree, self.listing, file_base, orphan_check, missing_check)

            self.assertEqual(len(missing), 1)
            self.assertTrue(m_size)
            self.assertEqual(len(orphan), 1)
            self.assertTrue(o_size)


class TestUnlisted(TestBase):

    unlisted_list = [
        ('/store/mc/ttThings/0000/_unlisted_', 0),
        ('/store/mc/ttThings/0001/zxcvb.root', 50),
        ('/store/data/runB/_unlisted_', 0),
        ('/store/data/runB/earlyfile.root', 5),
        ('/store/data/runA/0030/stuff.root', 10),
        ]
    
    def do_more_setup(self):
        self.unlisted_tree = datatypes.DirectoryInfo('/store')
        self.unlisted_tree.add_file_list(self.unlisted_list)

    def test_unlisted_missing(self):

        missing, _, _ = self.tree.compare(self.unlisted_tree)
        self.assertEqual(len(missing), 0)

    def test_unlisted_orphan(self):

        orphan, _, _ = self.unlisted_tree.compare(self.tree)
        self.assertEqual(len(orphan), 0)

    def test_unlisted_list(self):
        self.assertTrue('/store/mc/ttThings/0000' in self.unlisted_tree.get_unlisted())
        self.assertFalse('/store/mc/ttThings' in self.unlisted_tree.get_unlisted())
        self.assertEqual(len(self.unlisted_tree.get_unlisted()),
                         self.unlisted_tree.get_num_files(unlisted=True))

    def test_file_list(self):
        files = self.unlisted_tree.get_files()
        self.assertTrue('/store/mc/ttThings/0001/zxcvb.root' in files)
        self.assertTrue('/store/data/runB/earlyfile.root' in files)
        self.assertFalse('/store/mc/ttThings/0000/_unlisted_' in files)
        self.assertFalse(False in [f.endswith('.root') for f in files])
        self.assertEqual(len(files), 3)
        # Make sure that the min age for file list is working
        self.assertFalse(self.unlisted_tree.get_files(time.time() + 100))

        # Path behavior: Add name automatically, path parent directory to path argument
        subdir = self.unlisted_tree.get_node('mc').get_files(path='/store')
        self.assertTrue('/store/mc/ttThings/0001/zxcvb.root' in subdir)
        self.assertFalse('/store/data/runB/earlyfile.root' in subdir)


class TestUnfilled(TestBase):
    # This is for testing the tree behavior when some of the DirectoryInfo.files is None
    empty = [
        'mc/ttThings/empty/dir/a',
        'mc/ttThings/empty/dir/b',
        'mc/ttThings/empty/dir2'
        ]

    unfilled = 'mc/ttThings/empty/unfilled'

    def do_more_setup(self):
        # Add empty files
        for d in self.empty:
            self.tree.get_node(d).add_files([]).mtime = 1

        self.tree.setup_hash()

        for d in ['', 'dir', 'dir/a', 'dir/b', 'dir2']:
            self.tree.get_node(os.path.join('mc/ttThings/empty', d), make_new=False).mtime = 1

        self.tree.setup_hash()

        LOG.debug(self.tree.displays())

    def test_count(self):
        # We want unfilled directories to not change the total count
        first_count = self.tree.count_nodes()
        self.assertTrue(self.tree.get_node(self.unfilled).files is None)
        self.assertEqual(first_count, self.tree.count_nodes())

    def test_empty_list(self):
        empty_list = self.tree.empty_nodes_list()

        self.assertTrue('/store/mc/ttThings/empty/dir/a' in empty_list)
        self.assertTrue('/store/mc/ttThings/empty' in empty_list)

        new_node = self.tree.get_node(self.unfilled)

        self.tree.setup_hash()
        new_list = self.tree.empty_nodes_list()

        self.assertFalse('/store/' + self.unfilled in new_list)
        self.assertFalse('/store/mc/ttThings/empty' in new_list)

    def test_delete_dir(self):
        first_count = self.tree.count_nodes(empty=True)
        self.assertRaises(datatypes.NotEmpty, self.tree.remove_node, '/store/mc/ttThings/empty/dir')
        self.tree.remove_node('/store/mc/ttThings/empty/dir/a')
        self.assertEqual(first_count - 1, self.tree.count_nodes(empty=True))
        self.assertFalse('/store/mc/ttThings/empty/dir/a' in self.tree.empty_nodes_list())

        # Check that self.files is None throws an exception
        new_node = self.tree.get_node(self.unfilled)
        new_node.mtime = time.time()
        self.assertRaises(datatypes.NotEmpty, self.tree.remove_node, '/store/' + self.unfilled)
        new_node.files = []
        # Still no good because of mtime
        self.assertRaises(datatypes.NotEmpty, self.tree.remove_node, '/store/' + self.unfilled)
        # Now it should delete just fine
        new_node.mtime = 1
        self.tree.remove_node('/store/' + self.unfilled)

    def test_big_removal(self):
        self.assertTrue(self.tree.empty_nodes_list())
        for d in self.tree.empty_nodes_list():
            self.tree.remove_node(d)
        self.assertFalse(self.tree.empty_nodes_list())

    def test_new_empty(self):
        # Piggy-backing setup to check a bug
        self.tree.get_node('mc/ttThings/empty').mtime = time.time()
        self.assertFalse('/store/mc/ttThings/empty' in self.tree.empty_nodes_list())

    def test_nontime_subdir(self):
        self.tree.get_node('mc/ttThings/empty/dir/a').mtime = None
        empties = self.tree.empty_nodes_list()
        self.assertFalse('/store/mc/ttThings/empty/dir/a' in empties)
        self.assertFalse('/store/mc/ttThings/empty' in empties)
        self.assertTrue('/store/mc/ttThings/empty/dir/b' in empties)

    def test_noself_stillempty(self):
        # There's some bug that is giving empty nodes back when it shouldn't
        # This is just me trying to hunt it down
        self.tree.get_node('mc/ttThings/empty/dir/a').mtime = time.time()
        self.tree.setup_hash()
        empties = self.tree.empty_nodes_list()
        self.assertFalse('/store/mc/ttThings/empty/dir/a' in empties)
        self.assertFalse('/store/mc/ttThings/empty' in empties)
        self.assertTrue('/store/mc/ttThings/empty/dir/b' in empties)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        logging.basicConfig(level=logging.DEBUG)

    unittest.main()
