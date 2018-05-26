import unittest
import json
from bson import json_util
from fuse import FuseOSError

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

        # To ease some tests
        with open('test/resources/data/file.json','r') as f:
            self.file_raw = json_util.loads(f.read())
        self.file = self.obj.load_generic_file(self.file_raw)

        with open('test/resources/data/directory.json','r') as f:
            self.directory_raw = json_util.loads(f.read())
        self.directory = self.obj.load_generic_file(self.directory_raw)

        with open('test/resources/data/symbolic-link.json','r') as f:
            self.symbolic_link_raw = json_util.loads(f.read())
        self.symbolic_link = self.obj.load_generic_file(self.symbolic_link_raw)

        with open('test/resources/data/directory-file.json','r') as f:
            self.directory_file_raw = json_util.loads(f.read())
        self.directory_file = self.obj.load_generic_file(self.directory_file_raw)

    def tearDown(self):
        self.obj.clean_database()

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

    def test_master_lock_id(self):
        filename = 'test-file'
        hostname = Mongo.configuration.hostname()
        pid = 0
        expected_lock = filename + ';' + str(pid) + ';' + hostname
        self.assertEqual(self.obj.master_lock_id(filename=filename), expected_lock)

    def test_create_generic_file(self):
        self.obj.create_generic_file(generic_file=self.file)
        gf = self.obj.files_coll.find_one({'filename':self.file.filename},{'uploadDate':False})
        self.assertEqual(json_util.dumps(gf), json_util.dumps(self.file_raw))

    def test_remove_generic_file(self):
        self.obj.create_generic_file(generic_file=self.file)
        self.obj.remove_generic_file(generic_file=self.file)
        gf = self.obj.files_coll.find_one({'filename': self.file.filename})
        self.assertEqual(gf, None)

    def test_remove_generic_file_directory_not_empty(self):
        # Try to delete the parent directory while a file still exist in it
        self.obj.create_generic_file(generic_file=self.directory)
        self.obj.create_generic_file(generic_file=self.directory_file)
        try:
            self.obj.remove_generic_file(generic_file=self.directory)
            self.assertTrue(False)
        except FuseOSError as e:
            self.assertTrue(True)

    def test_remove_generic_file_directory_empty(self):
        # Try to delete the parent directory after deleting the file in it
        self.obj.create_generic_file(generic_file=self.directory)
        self.obj.create_generic_file(generic_file=self.directory_file)
        self.obj.remove_generic_file(generic_file=self.directory_file)
        self.obj.remove_generic_file(generic_file=self.directory)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()