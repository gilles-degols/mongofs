import unittest
import os
import subprocess
import time
from src.core.Configuration import Configuration

"""
    The purpose of this class is not to test the main.py directly, but rather do some integration tests with FUSE, to be
    sure that file system can be mounted, and that we can write/read/... to it.
    This is super-ugly as code, but useful at least.
"""
class TestIntegration(unittest.TestCase):
    TEST_DIRECTORY = '/mnt/mongofs-integration-test-56789456'
    START_COMMAND = 'nohup python3.6 -m src.main /mnt/data test/resources/conf/mongofs.json >/dev/null 2>&1 &'

    def setUp(self):
        commands = [
            'mkdir -p '+TestIntegration.TEST_DIRECTORY,
            'rm -rf '+TestIntegration.TEST_DIRECTORY+'/*'
        ]
        for command in commands:
            self.execute_command(command=command)
        self.execute_background_command(command=TestIntegration.START_COMMAND)

    def execute_command(self, command):
        res = subprocess.run([command], shell=True, stdout=subprocess.PIPE)
        res.stdout = res.stdout.decode('utf-8')
        return res

    def execute_background_command(self, command):
        subprocess.run([command], shell=True, stdout=subprocess.PIPE)
        # We cannot be sure that the file system is directly accessible (it might take a few ms). So we verify it.
        for i in range(0, 10):
            touch_result = self.execute_command(command='touch '+TestIntegration.TEST_DIRECTORY+'/hello-start')
            if touch_result.returncode != 0:
                time.sleep(0.05*(i+1))
            else:
                return True
        exit(1)

    def kill_background_command(self, command):
        self.execute_command(command='pkill -f "'+command+'"')

    def tearDown(self):
        self.kill_background_command(command=TestIntegration.START_COMMAND)

    def test_touch(self):
        res = self.execute_command(command='touch '+TestIntegration.TEST_DIRECTORY+'/hello')
        self.assertEqual(res.returncode, 0)

if __name__ == '__main__':
    unittest.main()