import unittest
from src.core.Configuration import Configuration
from src.core.Mongo import Mongo
from src.core.GenericFile import GenericFile
from stat import S_IFDIR

class TestRootMode(unittest.TestCase):
    def setUpConfig(self, path):
        Configuration.FILEPATH = path
        GenericFile.configuration = Configuration()
        self.obj = Mongo(do_clean_up=True)
        GenericFile.mongo = self.obj

    def tearDown(self):
        self.obj.clean_database()

    def test_default_rootMode(self):
        self.setUpConfig('test/resources/conf/mongofs.json')
        self.assertEqual(GenericFile.configuration.default_root_mode(), 0o755)
        self.assertEqual(GenericFile.configuration.force_root_mode(), False)
        root = self.obj.get_generic_file(filepath='/')
        self.assertEqual(root.metadata['st_mode'] & 0o777, 0o755)
        self.assertEqual(root.metadata['st_mode'] & S_IFDIR, S_IFDIR)

    def test_special_rootMode(self):
        self.setUpConfig('test/resources/conf/mongofs-rootMode777.json')
        self.assertEqual(GenericFile.configuration.default_root_mode(), 0o777)
        self.assertEqual(GenericFile.configuration.force_root_mode(), True)
        root = self.obj.get_generic_file(filepath='/')
        self.assertEqual(root.metadata['st_mode'] & 0o777, 0o777)
        self.assertEqual(root.metadata['st_mode'] & S_IFDIR, S_IFDIR)

    def test_force_rootMode(self):
        self.setUpConfig('test/resources/conf/mongofs.json')
        root = self.obj.get_generic_file(filepath='/')
        self.assertEqual(root.metadata['st_mode'] & 0o777, 0o755)
        self.assertEqual(root.metadata['st_mode'] & S_IFDIR, S_IFDIR)

        # Now change to force mode to 777, it should update the existing data for /
        self.setUpConfig('test/resources/conf/mongofs-rootMode777.json')
        root = self.obj.get_generic_file(filepath='/')
        self.assertEqual(root.metadata['st_mode'] & 0o777, 0o777)
        self.assertEqual(root.metadata['st_mode'] & S_IFDIR, S_IFDIR)
