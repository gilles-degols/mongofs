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
        self.mongo = Mongo(do_clean_up=True)
        GenericFile.mongo = self.mongo
        GenericFile.configuration = Configuration()
        self.utils = Utils(mongo=self.mongo)
        self.utils.load_files()

    def tearDown(self):
        self.mongo.clean_database()

    def test_get_target(self):
        self.assertEqual(self.utils.symbolic_link.get_target(), self.utils.symbolic_link_raw['target'])

    def test_is_file(self):
        self.assertFalse(self.utils.symbolic_link.is_file())

    def test_is_dir(self):
        self.assertFalse(self.utils.symbolic_link.is_dir())

    def test_is_link(self):
        self.assertTrue(self.utils.symbolic_link.is_link())
        self.assertFalse(self.utils.directory.is_link())
        self.assertFalse(self.utils.file.is_link())
        self.assertFalse(self.utils.directory_file.is_link())

if __name__ == '__main__':
    unittest.main()