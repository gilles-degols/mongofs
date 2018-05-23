#!/usr/bin/env python
import logging

from sys import argv, exit
from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from core.Configuration import Configuration
from core.GenericFile import GenericFile
from core.File import File
from core.Mongo import Mongo

"""
    Simulate a file system running on MongoDB.
    A simple example to start: https://github.com/terencehonles/fusepy/blob/master/examples/memory.py
"""
class MongoFS(LoggingMixIn, Operations):
    def __init__(self):
        self.configuration = Configuration()
        self.mongo = Mongo()
        self.files = {}

        # Additional setup
        GenericFile.mongo = self.mongo

        # START DEBUG ONLY - Drop the old information from MongoDB
        self.mongo.clean_database()
        # END DEBUG ONLY

        # We need to be sure to have the top folder created in MongoDB
        self.mkdir(path='/', mode=0o755)

    """
        Create a file and returns a "file descriptor", which is in fact, simply the _id.
    """
    def create(self, path, mode):
        f = GenericFile.new_generic_file(filename=path, mode=mode, file_type=GenericFile.FILE_TYPE)
        return f.file_descriptor

    """
        Delete a file
    """
    def unlink(self, path):
        raw_file = self.mongo.get_generic_file(filename=path)
        gf = GenericFile(obj=raw_file)
        self.mongo.remove_generic_file(generic_file=gf)

    """
        Create a directory, no need to return anything
    """
    def mkdir(self, path, mode):
        GenericFile.new_generic_file(filename=path, mode=mode, file_type=GenericFile.DIRECTORY_TYPE)

    """
        Delete a directory
    """
    def rmdir(self, path):
        raw_file = self.mongo.get_generic_file(filename=path)
        gf = GenericFile(obj=raw_file)
        self.mongo.remove_generic_file(generic_file=gf)

    """
        List files inside a directory
    """
    def readdir(self, path, fh):
        filenames = self.mongo.list_filenames(directory=path)

        # We need to only keep final filename
        filenames = [file.split('/')[-1] for file in filenames]
        return ['.', '..'] + filenames

    """
        Write data to a file, from a specific offset. Returns the written data size
    """
    def write(self, path, data, offset, fh):
        raw_file = self.mongo.get_generic_file(filename=path)
        f = File(obj=raw_file)
        f.add_data(data=data, offset=offset)
        return len(data)

    """
        Truncate a file to a specific length
    """
    def truncate(self, path, length, fh=None):
        raw_file = self.mongo.get_generic_file(filename=path)
        f = File(obj=raw_file)
        f.truncate(length=length)

    """
        Return general information for a given path
    """
    def getattr(self, path, fh=None):
        f = self.mongo.get_generic_file(filename=path)
        if f is None:
            raise FuseOSError(ENOENT)

        return f.metadata

    def removexattr(self, path, name):
        pass

if __name__ == '__main__':
    if len(argv) < 2:
        print('usage: %s <mountpoint> (<configuration_filepath>)' % argv[0])
        exit(1)

    if len(argv) == 3:
        configuration_filepath = argv[2]
        Configuration.FILEPATH = configuration_filepath

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(MongoFS(), argv[1], foreground=True)