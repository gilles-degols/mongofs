import unittest
import json
from bson import json_util
from fuse import FuseOSError
from stat import S_IFDIR, S_IFLNK, S_IFREG

from src.core.Configuration import Configuration
from src.core.Mongo import Mongo
from src.core.GenericFile import GenericFile
from src.core.File import File
from src.core.Directory import Directory
from src.core.SymbolicLink import SymbolicLink

from test.core.Utils import Utils

class TestGenericFile(unittest.TestCase):
    def setUp(self):
        Configuration.FILEPATH = 'test/resources/conf/mongofs.json'
        self.mongo = Mongo()
        GenericFile.mongo = self.mongo
        self.utils = Utils(mongo=self.mongo)
        self.utils.load_files()

    def tearDown(self):
        self.mongo.clean_database()

    def test_basic_save(self):
        self.utils.insert_file()
        initial_gf = self.mongo.files_coll.find_one({'filename':self.utils.file.filename},{'uploadDate':False})

        self.utils.file.metadata['st_nlink'] = 37
        self.utils.file.basic_save()
        modified_gf = self.mongo.files_coll.find_one({'filename': self.utils.file.filename}, {'uploadDate': False})

        self.assertEqual(modified_gf['metadata']['st_nlink'], 37)

    # We do not test is_dir / is_file / is_link here as we cannot load a GenericFile only, so the test would have no purpose.
    # We need to test those methods in test_File, test_Directory and test_SymbolicLink.

    def test_rename_to(self):
        self.utils.insert_file()
        initial_filename = self.utils.file.filename
        self.utils.file.rename_to(filename='rename-file')
        old_file = self.mongo.files_coll.find_one({'filename': initial_filename})
        self.assertEqual(old_file, None)

        new_file = self.mongo.files_coll.find_one({'filename': 'rename-file'})
        self.assertNotEqual(new_file, None)

    def test_unlock(self):
        self.utils.insert_file()
        gf = self.mongo.get_generic_file(filename=self.utils.file.filename, take_lock=True)
        self.assertTrue('lock' in gf.json)
        gf.unlock()

        gf = self.mongo.get_generic_file(filename=self.utils.file.filename)
        self.assertTrue('lock' not in gf.json)

    def test_new_generic_file_file(self):
        # Creation of a basic file
        GenericFile.new_generic_file(filename=self.utils.file.filename, mode=0o755, file_type=GenericFile.FILE_TYPE)
        inserted_file = self.mongo.files_coll.find_one({'filename':self.utils.file.filename})
        self.assertEqual(inserted_file['filename'], self.utils.file.filename)
        self.assertEqual(inserted_file['metadata']['st_mode'], (S_IFREG | 0o755))
        self.assertEqual(inserted_file['generic_file_type'], GenericFile.FILE_TYPE)

    def test_new_generic_file_directory(self):
        # Creation of a directory
        GenericFile.new_generic_file(filename=self.utils.directory.filename, mode=0o755, file_type=GenericFile.DIRECTORY_TYPE)
        inserted_file = self.mongo.files_coll.find_one({'filename':self.utils.directory.filename})
        self.assertEqual(inserted_file['filename'], self.utils.directory.filename)
        self.assertEqual(inserted_file['metadata']['st_mode'], (S_IFDIR | 0o755))
        self.assertEqual(inserted_file['generic_file_type'], GenericFile.DIRECTORY_TYPE)

    def test_new_generic_file_symbolic_link(self):
        # Creation of a symbolic link to the initial self.utils.file just below
        self.utils.insert_file()

        GenericFile.new_generic_file(filename=self.utils.symbolic_link.filename, mode=0o755, file_type=GenericFile.SYMBOLIC_LINK_TYPE, target=self.utils.file.filename)
        inserted_file = self.mongo.files_coll.find_one({'filename':self.utils.symbolic_link.filename})
        self.assertEqual(inserted_file['filename'], self.utils.symbolic_link.filename)
        self.assertEqual(inserted_file['metadata']['st_mode'], (S_IFLNK | 0o755))
        self.assertEqual(inserted_file['generic_file_type'], GenericFile.SYMBOLIC_LINK_TYPE)
        self.assertEqual(inserted_file['target'], self.utils.file.filename)

    def test_get_directory(self):
        self.utils.insert_directory()

        directory_name = GenericFile.get_directory(filename=self.utils.directory.filename)
        self.assertEqual(directory_name, "/")

    def test_is_generic_filename_available(self):
        self.utils.insert_file()

        is_available = GenericFile.is_generic_filename_available(filename=self.utils.file.filename)
        self.assertFalse(is_available)

        is_available = GenericFile.is_generic_filename_available(filename=self.utils.file.filename+'.something')
        self.assertTrue(is_available)

        # There is no need to verify if a filename is available inside a non-existing directory , it will be automatically
        # checked by FUSE with readdir()

if __name__ == '__main__':
    unittest.main()