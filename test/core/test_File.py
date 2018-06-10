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

    # The tests below are mostly taken from test_Mongo as we need to check the same thing... A few changes have been
    # made to be sure to the test the right interface (GenericFile and not Mongo)
    def test_add_data_append(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        self.utils.file.add_data(data=b'test', offset=len(message))
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

        self.utils.file.add_data(data=b'things', offset=2)
        modified_message = self.utils.read_file_chunks()
        formatted_modified_message = ''.join(map(chr, list(modified_message)))
        self.assertEqual(formatted_modified_message, expected_message)

    def test_read_data(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        data = self.utils.file.read_data(offset=0, size=4096)
        self.assertEqual(data, message)

        data = self.utils.file.read_data(offset=3, size=4096)
        self.assertEqual(data, message[3:])

        data = self.utils.file.read_data(offset=0, size=8)
        self.assertEqual(data, message[:8])

        data = self.utils.file.read_data(offset=3, size=8)
        self.assertEqual(data, message[3:3+8])

    def test_truncate(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        self.utils.file.truncate(length=6)
        modified_message = self.utils.read_file_chunks()
        self.assertEqual(modified_message, message[0:6])

    def test_truncate_zero(self):
        self.utils.insert_file()
        self.utils.insert_file_chunks()
        message = self.utils.read_file_chunks()

        self.utils.file.truncate(length=0)
        modified_message = self.utils.read_file_chunks()
        self.assertEqual(modified_message, message[0:0])

    def test_is_file(self):
        self.assertTrue(self.utils.file.is_file())
        self.assertFalse(self.utils.directory.is_file())
        self.assertFalse(self.utils.symbolic_link.is_file())
        self.assertTrue(self.utils.directory_file.is_file())

    def test_is_dir(self):
        self.assertFalse(self.utils.file.is_dir())

    def test_is_link(self):
        self.assertFalse(self.utils.file.is_link())

if __name__ == '__main__':
    unittest.main()