import unittest
from unittest.mock import patch
from bson import json_util
from fuse import FuseOSError

from src.core.Configuration import Configuration
from src.core.Mongo import Mongo
from src.core.GenericFile import GenericFile
from src.core.File import File
from src.core.Directory import Directory
from src.core.SymbolicLink import SymbolicLink

from test.core.Utils import Utils

class TestMongo(unittest.TestCase):
    def setUp(self):        
        Configuration.FILEPATH = 'test/resources/conf/mongofs.json'
        self.obj = Mongo(do_clean_up=True)
        GenericFile.mongo = self.obj
        GenericFile.configuration = Configuration()
        self.utils = Utils(mongo=self.obj)
        self.utils.load_files()

    def tearDown(self):
        self.obj.clean_database()

    def test_load_generic_file_file(self):
        self.assertIsInstance(self.obj.load_generic_file(self.utils.file_raw), File)

    def test_load_generic_file_directory(self):
        self.assertIsInstance(self.obj.load_generic_file(self.utils.directory_raw), Directory)

    def test_load_generic_file_symbolic_link(self):
        self.assertIsInstance(self.obj.load_generic_file(self.utils.symbolic_link_raw), SymbolicLink)

    def test_current_user(self):
        user = self.obj.current_user()
        self.assertIsInstance(user['uid'], int)
        self.assertIsInstance(user['gid'], int)
        self.assertIsInstance(user['pid'], int)

    def test_lock_id(self):
        filepath = 'test-file'
        hostname = Mongo.configuration.hostname()
        pid = self.obj.current_user()['pid']
        expected_lock = filepath+';'+str(pid)+';'+hostname
        self.assertEqual(self.obj.lock_id(filepath=filepath), expected_lock)

    def test_create_generic_file(self):
        self.utils.insert_file()
        gf = self.utils.files_coll.find_one({'directory_id':self.utils.file.directory_id,'filename':self.utils.file.filename},{'uploadDate':False})
        self.assertEqual(json_util.dumps(gf, sort_keys=True), json_util.dumps(self.utils.file_raw, sort_keys=True))

    def test_remove_generic_file(self):
        self.utils.insert_file()
        self.obj.remove_generic_file(generic_file=self.utils.file)
        gf = self.utils.files_coll.find_one({'filename': self.utils.file.filename})
        self.assertEqual(gf, None)

    def test_remove_generic_file_directory_not_empty(self):
        # Try to delete the parent directory while a file still exist in it
        self.utils.insert_directory()
        self.obj.create_generic_file(generic_file=self.utils.directory_file)
        try:
            self.obj.remove_generic_file(generic_file=self.utils.directory)
            self.assertTrue(False, msg="It was possible to remove a directory while it was still containing files.")
        except FuseOSError as e:
            self.assertTrue(True)

    def test_remove_generic_file_directory_empty(self):
        # Try to delete the parent directory after deleting the file in it
        self.utils.insert_directory()
        self.utils.insert_directory_file()
        self.obj.remove_generic_file(generic_file=self.utils.directory_file)
        self.obj.remove_generic_file(generic_file=self.utils.directory)
        self.assertTrue(True)

    def test_list_generic_files_in_directory(self):
        self.utils.insert_directory()
        self.utils.insert_file()
        self.utils.insert_symbolic_link()

        files = self.obj.list_generic_files_in_directory(filepath='/')
        self.assertEqual(len(files), 3)

    def test_generic_file_exists(self):
        self.assertFalse(self.obj.generic_file_exists(self.utils.file.filepath))
        self.utils.insert_file()
        self.assertTrue(self.obj.generic_file_exists(self.utils.file.filepath))

    def test_get_generic_file(self):
        self.utils.insert_file()
        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath)
        self.assertIsInstance(gf, File)

    def test_get_generic_file_take_lock(self):
        self.utils.insert_file()
        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_WRITE})
        self.assertIsInstance(gf, File)
        self.assertTrue('lock' in gf.json)
        self.assertEqual(len(gf.json['lock']), 1)
        self.assertEqual(gf.json['lock'][0]['type'], GenericFile.LOCK_WRITE)

        # We are the same owner, so normally, we should still be able to take the file if there is a lock on it.
        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_READ})
        self.assertIsInstance(gf, File)
        self.assertTrue('lock' in gf.json)
        self.assertEqual(len(gf.json['lock']), 1)
        self.assertEqual(gf.json['lock'][0]['type'], GenericFile.LOCK_READ)

        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_UNLOCK})
        self.assertIsInstance(gf, File)
        self.assertFalse('lock' in gf.json)

    def test_get_file_multiple_lock_process(self):
        with patch.object(self.obj, 'current_user') as mock_current_user:
            mock_current_user.return_value = self.obj.user(1, 1, 1)
            self.utils.insert_file()
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_WRITE})
            self.assertIsInstance(gf, File)
            self.assertTrue('lock' in gf.json)
            self.assertEqual(len(gf.json['lock']), 1)
            self.assertEqual(gf.json['lock'][0]['type'], GenericFile.LOCK_WRITE)

            mock_current_user.return_value = self.obj.user(2, 2, 2)
            with self.assertRaises(FuseOSError):
                self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_WRITE})
            with self.assertRaises(FuseOSError):
                self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_READ})

            mock_current_user.return_value = self.obj.user(1, 1, 1)
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_READ})
            self.assertIsInstance(gf, File)
            self.assertEqual(len(gf.json['lock']), 1)
            self.assertEqual(gf.json['lock'][0]['type'], GenericFile.LOCK_READ)

            mock_current_user.return_value = self.obj.user(2, 2, 2)
            with self.assertRaises(FuseOSError):
                self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_WRITE})
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_READ})
            self.assertIsInstance(gf, File)
            self.assertTrue('lock' in gf.json)
            self.assertEqual(len(gf.json['lock']), 2)
            self.assertEqual(gf.json['lock'][0]['type'], GenericFile.LOCK_READ)
            self.assertEqual(gf.json['lock'][1]['type'], GenericFile.LOCK_READ)
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_UNLOCK})
            self.assertTrue('lock' in gf.json)
            self.assertEqual(len(gf.json['lock']), 1)

            self.obj.release_generic_file(filepath=self.utils.file.filepath, generic_file=gf)
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath)
            self.assertTrue('lock' in gf.json)
            self.assertEqual(len(gf.json['lock']), 1)

            self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_READ})
            mock_current_user.return_value = self.obj.user(1, 1, 1)
            self.obj.release_generic_file(filepath=self.utils.file.filepath, generic_file=gf)
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath)
            self.assertTrue('lock' in gf.json)
            self.assertEqual(len(gf.json['lock']), 1)

            with self.assertRaises(FuseOSError):
                self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_UNLOCK})

            mock_current_user.return_value = self.obj.user(2, 2, 2)
            gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_UNLOCK})
            self.assertTrue('lock' not in gf.json)
            

    def test_get_generic_file_missing(self):
        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath)
        self.assertEqual(gf, None)

    def test_add_nlink_directory(self):
        # By default, a directory has 2 st_nlink. And by default, the "/" directory always exists.
        self.obj.add_nlink_directory(directory_id=self.utils.root_id, value=4)
        gf = self.utils.files_coll.find_one({'_id': self.utils.root_id})
        self.assertEqual(gf['metadata']['st_nlink'], 6)

    def test_read_data(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        data = self.obj.read_data(file=self.utils.file, offset=0, size=4096)
        self.assertEqual(data, message)

        data = self.obj.read_data(file=self.utils.file, offset=3, size=4096)
        self.assertEqual(data, message[3:])

        data = self.obj.read_data(file=self.utils.file, offset=0, size=8)
        self.assertEqual(data, message[:8])

        data = self.obj.read_data(file=self.utils.file, offset=3, size=8)
        self.assertEqual(data, message[3:3+8])

    def test_add_data_append(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        self.obj.add_data(file=self.utils.file, data=b'test', offset=len(message))
        modified_message = self.utils.read_file_chunks()
        self.assertEqual(modified_message, message+b'test')

    def test_add_data_replace(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()
        message = list(message)
        message[2] = ord('t')
        message[3] = ord('h')
        message[4] = ord('i')
        message[5] = ord('n')
        message[6] = ord('g')
        message[7] = ord('s')
        expected_message = ''.join(map(chr, message))

        self.obj.add_data(file=self.utils.file, data=b'things', offset=2)
        modified_message = self.utils.read_file_chunks()
        formatted_modified_message = ''.join(map(chr, list(modified_message)))
        self.assertEqual(formatted_modified_message, expected_message)

    def test_truncate(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        self.obj.truncate(file=self.utils.file, length=6)
        modified_message = self.utils.read_file_chunks()
        self.assertEqual(modified_message, message[0:6])

    def test_truncate_zero(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        self.obj.truncate(file=self.utils.file, length=0)
        modified_message = self.utils.read_file_chunks()
        self.assertEqual(modified_message, message[0:0])

    def test_rename_generic_file_to(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        initial_filepath = self.utils.file.filepath
        self.obj.rename_generic_file_to(generic_file=self.utils.file, initial_filepath=self.utils.file.filepath, destination_filepath='/rename-test')
        destination_message = self.utils.read_file_chunks() # Read chunks is based on the _id, so we can call it
        # Normally, the chunks should not change at all, but we never know.
        self.assertEqual(destination_message, message)

        old_file = self.utils.files_coll.find_one({'directory_id':self.utils.file.directory_id,'filename':initial_filepath.split('/')[-1]})
        self.assertEqual(old_file, None)

        new_file = self.utils.files_coll.find_one({'directory_id':self.utils.root_id,'filename':'rename-test'})
        self.assertNotEqual(new_file, None)

    def test_release_generic_file(self):
        self.utils.insert_file()
        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock={'type': GenericFile.LOCK_WRITE})
        result = self.obj.release_generic_file(filepath=self.utils.file.filepath, generic_file=gf)
        self.assertTrue(result)

        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath)
        self.assertTrue('lock' not in gf.json or len(gf.json['lock']) == 0)

    def test_release_generic_file_no_lock(self):
        # Verify if it does not crash if there is no lock
        self.utils.insert_file()
        gf = self.obj.get_generic_file(filepath=self.utils.file.filepath, lock=None)
        result = self.obj.release_generic_file(filepath=self.utils.file.filepath, generic_file=gf)
        self.assertTrue(result)

    def test_basic_save(self):
        self.utils.insert_file()
        self.obj.basic_save(generic_file=self.utils.file, metadata={'st_nlink':1}, attrs={'thing':1}, host=self.utils.file.host, gname=self.utils.file.gname, uname=self.utils.file.uname)
        result = self.utils.files_coll.find_one({'_id':self.utils.file._id})
        self.assertTrue('st_nlink' in result['metadata'] and len(result['metadata']) == 1)
        self.assertTrue('thing' in result['attrs'] and len(result['attrs']) == 1)


if __name__ == '__main__':
    unittest.main()