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
    TEST_DIRECTORY = '/mnt/mongofs-integration-test'
    START_COMMAND = 'nohup python3.6 -m src.main '+TEST_DIRECTORY+' test/resources/conf/mongofs.json >/dev/null 2>&1 &'

    def setUp(self):
        commands = [
            'mkdir -p '+TestIntegration.TEST_DIRECTORY,
            'rm -rf '+TestIntegration.TEST_DIRECTORY+'/*'
        ]
        for command in commands:
            self.execute_command(command=command)
        self.execute_background_command(command=TestIntegration.START_COMMAND)

    def execute_command(self, command):
        res = subprocess.run([command], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True)
        return res

    def execute_background_command(self, command):
        subprocess.run([command], shell=True, stdout=subprocess.PIPE)
        # We cannot be sure that the file system is directly accessible (it might take a few ms). And we could be trying
        # to read to the local file system instead, so we need to verify if the mounting was correctly done.
        for i in range(0, 10):
            touch_result = self.execute_command(command='mountpoint -q '+TestIntegration.TEST_DIRECTORY)
            if touch_result.returncode != 0:
                time.sleep(0.05*(i+1))
            else:
                return True
        print('Impossible to mount the file system in the given time. Abort.')
        exit(1)

    def kill_background_command(self, command):
        self.execute_command(command='pkill -9 -f "'+command+'"')

    def close_mount(self):
        # Some work to be able to really close the file system correctly...
        fuse_command = 'fusermount -u '+TestIntegration.TEST_DIRECTORY
        rmdir_command = 'rmdir '+TestIntegration.TEST_DIRECTORY
        for i in range(0, 100):
            self.execute_command(command=fuse_command)
            self.execute_command(command=rmdir_command)
            directory_above = '/'.join(TestIntegration.TEST_DIRECTORY.split('/')[0:-1])
            files = self.list_directory_content(directory=directory_above, absolute=True)

            found_directory = False
            for file in files:
                if file['filename'] == TestIntegration.TEST_DIRECTORY.split('/')[-1]:
                    found_directory = True
            if found_directory is True:
                time.sleep(0.05 * (i + 1))
            else:
                return True
        print('Impossible to umount the file system in the given time. Abort.')
        exit(1)

    def tearDown(self):
        self.close_mount()
        # It might be necessary some times...
        self.kill_background_command(command=TestIntegration.START_COMMAND)

    def list_directory_content(self, directory='', absolute=False):
        if absolute is True:
            res = self.execute_command(command='ls ' + directory + ' -lv')
        else:
            res = self.execute_command(command='ls ' + TestIntegration.TEST_DIRECTORY + '/' + directory + ' -lv')
        files = []
        for elem in res.stdout.split('\n'):
            info = elem.strip().split()
            if len(info) <= 5 or (len(info) >= 1 and info[0] == 'ls:'):
                # The first line is not interesting (with a "total" information)
                # If there was a mount/umount problem you can also have: ls: cannot access mongofs-integration-test-0.653900736406249: Transport endpoint is not connected
                continue
            attributes = info[0]
            owner = info[2]
            group = info[3]
            size = int(info[4])
            filename = info[-1]
            file = {'attributes':attributes,'owner':owner,'group':group,'size':size, 'filename':filename}
            files.append(file)

        return files

    def test_touch(self):
        res = self.execute_command(command='touch '+TestIntegration.TEST_DIRECTORY+'/hello')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'hello')

    def test_rm(self):
        self.execute_command(command='touch '+TestIntegration.TEST_DIRECTORY+'/hello')
        res = self.execute_command(command='rm '+TestIntegration.TEST_DIRECTORY+'/hello')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 0)

    def test_write(self):
        res = self.execute_command(command='echo "some text" > ' + TestIntegration.TEST_DIRECTORY + '/hello')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'hello')
        self.assertEqual(files[0]['size'], len("some test\n"))

    def test_read(self):
        self.execute_command(command='echo "some text" >> ' + TestIntegration.TEST_DIRECTORY + '/hello')
        res = self.execute_command(command='cat ' + TestIntegration.TEST_DIRECTORY + '/hello')
        self.assertEqual(res.stdout.strip(), "some text")

    def test_mkdir(self):
        res = self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'hello')
        self.assertEqual(files[0]['attributes'][0], "d")

    def test_mkdir_and_file(self):
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        res = self.execute_command(command='touch ' + TestIntegration.TEST_DIRECTORY + '/hello/subfile')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content(directory='hello/')
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'subfile')

    def test_rmdir_empty(self):
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        res = self.execute_command(command='rmdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 0)

    def test_rmdir_full(self):
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        self.execute_command(command='touch ' + TestIntegration.TEST_DIRECTORY + '/hello/subfile')
        res = self.execute_command(command='rmdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        self.assertEqual(res.returncode, 1)
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)

    def test_symbolic_link(self):
        self.execute_command(command='echo "some text" >> ' + TestIntegration.TEST_DIRECTORY + '/hello')
        res = self.execute_command(command='ln -s ' + TestIntegration.TEST_DIRECTORY + '/hello '+ TestIntegration.TEST_DIRECTORY + '/link')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 2)
        res = self.execute_command(command='cat ' + TestIntegration.TEST_DIRECTORY + '/link')
        self.assertEqual(res.stdout.strip(), "some text")

    def test_symbolic_link_invalid_target(self):
        # It is normal to return "0" even if the target does not exist, this is the same behavior in other file systems.
        res = self.execute_command(command='ln -s ' + TestIntegration.TEST_DIRECTORY + '/hello '+ TestIntegration.TEST_DIRECTORY + '/link')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)

    def test_file_rename_file(self):
        self.execute_command(command='echo "some text" >> ' + TestIntegration.TEST_DIRECTORY + '/hello')
        res = self.execute_command(command='mv ' + TestIntegration.TEST_DIRECTORY + '/hello ' + TestIntegration.TEST_DIRECTORY + '/hello2')
        self.assertEqual(res.returncode, 0)
        res = self.execute_command(command='cat ' + TestIntegration.TEST_DIRECTORY + '/hello2')
        self.assertEqual(res.returncode, 0)
        self.assertEqual(res.stdout.strip(), "some text")
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'hello2')

    def test_file_rename_directory_empty(self):
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/hello')
        res = self.execute_command(command='mv ' + TestIntegration.TEST_DIRECTORY + '/hello ' + TestIntegration.TEST_DIRECTORY + '/hello2')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'hello2')
        self.assertEqual(files[0]['attributes'][0], "d")

    def test_file_rename_directory_full(self):
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/dir0')
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/dir0/dir1')
        self.execute_command(command='mkdir ' + TestIntegration.TEST_DIRECTORY + '/dir0/dir1/dir2')
        self.execute_command(command='echo "some text" >> ' + TestIntegration.TEST_DIRECTORY + '/dir0/dir1/dir2/hello')
        res = self.execute_command(command='mv ' + TestIntegration.TEST_DIRECTORY + '/dir0 ' + TestIntegration.TEST_DIRECTORY + '/renamed-dir0')
        self.assertEqual(res.returncode, 0)
        files = self.list_directory_content()
        # print('List files in /: \n'+str(files))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'renamed-dir0')
        self.assertEqual(files[0]['attributes'][0], "d")

        files = self.list_directory_content(directory='renamed-dir0')
        # print('List files in renamed-dir0: \n'+str(files))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'dir1')
        self.assertEqual(files[0]['attributes'][0], "d")

        files = self.list_directory_content(directory='renamed-dir0/dir1')
        # print('\nList files in renamed-dir0/dir1: \n'+str(files))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'dir2')
        self.assertEqual(files[0]['attributes'][0], "d")

        files = self.list_directory_content(directory='renamed-dir0/dir1/dir2')
        # print('\nList files in renamed-dir0/dir1/dir2: \n'+str(files))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['filename'], 'hello')
        self.assertNotEqual(files[0]['attributes'][0], "d")

        res = self.execute_command(command='cat ' + TestIntegration.TEST_DIRECTORY + '/renamed-dir0/dir1/dir2/hello')
        self.assertEqual(res.returncode, 0)
        self.assertEqual(res.stdout.strip(), "some text")


if __name__ == '__main__':
    unittest.main()