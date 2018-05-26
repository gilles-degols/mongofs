import unittest
import json
from src.core.Configuration import Configuration
from src.core.Mongo import Mongo
from src.core.GenericFile import GenericFile
from src.core.File import File
from src.core.Directory import Directory
from src.core.SymbolicLink import SymbolicLink

class TestMongo(unittest.TestCase):
    def setUp(self):
        Configuration.FILEPATH = 'test/resources/conf/mongofs.json'
        self.obj = Mongo()
        self.obj.clean_database()

    def tearDown(self):
        pass

    def test_connect(self):
        # Normally the "setUp" should have already created a connection
        self.obj.connect()
        self.assertEqual(list(self.obj.files_coll.find({'_id':'0'})), [])

    def test_load_generic_file_file(self):
        with open('test/resources/data/file.json','r') as f:
            raw = json.load(f)
        self.assertIsInstance(self.obj.load_generic_file(raw), File)

    def test_load_generic_file_directory(self):
        with open('test/resources/data/directory.json','r') as f:
            raw = json.load(f)
        self.assertIsInstance(self.obj.load_generic_file(raw), Directory)

    def test_load_generic_file_symbolic_link(self):
        with open('test/resources/data/symbolic-link.json','r') as f:
            raw = json.load(f)
        self.assertIsInstance(self.obj.load_generic_file(raw), SymbolicLink)

    def test_current_user(self):
        user = self.obj.current_user()
        self.assertIsInstance(user['uid'], int)
        self.assertIsInstance(user['gid'], int)
        self.assertIsInstance(user['pid'], int)

    def test_lock_id(self):
        filename = 'test-file'
        hostname = Mongo.configuration.hostname()
        pid = self.obj.current_user()['pid']
        expected_lock = filename+';'+str(pid)+';'+hostname
        self.assertEqual(self.obj.lock_id(filename=filename), expected_lock)

if __name__ == '__main__':
    unittest.main()