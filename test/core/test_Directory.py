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
        GenericFile.configuration = Configuration()
        self.utils = Utils(mongo=self.mongo)
        self.utils.load_files()

    def tearDown(self):
        self.mongo.clean_database()

    def test_is_file(self):
        self.assertFalse(self.utils.directory.is_file())

    def test_is_dir(self):
        self.assertTrue(self.utils.directory.is_dir())
        self.assertFalse(self.utils.file.is_dir())
        self.assertFalse(self.utils.symbolic_link.is_dir())
        self.assertFalse(self.utils.directory_file.is_dir())

    def test_is_link(self):
        self.assertFalse(self.utils.directory.is_link())

if __name__ == '__main__':
    unittest.main()