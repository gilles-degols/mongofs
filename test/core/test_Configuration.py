import unittest
from src.core.Configuration import Configuration

class TestConfiguration(unittest.TestCase):
    def setUp(self):
        Configuration.FILEPATH = 'test/resources/conf/mongofs.json'
        self.obj = Configuration()

    def tearDown(self):
        pass

    def test_mongo_hosts(self):
        self.assertEqual(self.obj.mongo_hosts(), ["127.0.0.1:27017"])

    def test_mongo_database(self):
        self.assertEqual(self.obj.mongo_database(), "mongofsTests")

    def test_mongo_prefix(self):
        self.assertEqual(self.obj.mongo_prefix(), "mongofsTests_")

    def test_mongo_access_attempt(self):
        self.assertEqual(self.obj.mongo_access_attempt(), 6)

    def test_mongo_access_attempt_infinite(self):
        self.obj.conf['mongo']['access_attempt_s'] = 0
        self.assertTrue(self.obj.mongo_access_attempt() >= 3600*24*365)

    def test_lock_timeout(self):
        self.assertEqual(self.obj.lock_timeout(), 6)

    def test_lock_timeout_infinite(self):
        self.obj.conf['lock']['timeout_s'] = 0
        self.assertTrue(self.obj.lock_timeout() >= 3600*24*365)

    def test_lock_access_attempt(self):
        self.assertEqual(self.obj.lock_access_attempt(), 6)

    def test_lock_access_attempt_infinite(self):
        self.obj.conf['lock']['access_attempt_s'] = 0
        self.assertTrue(self.obj.lock_access_attempt() >= 3600*24*365)

    def test_hostname(self):
        self.assertEqual(self.obj.hostname(), "localhost")

if __name__ == '__main__':
    unittest.main()