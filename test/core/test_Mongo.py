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

if __name__ == '__main__':
    unittest.main()