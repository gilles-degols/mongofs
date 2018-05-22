#!/usr/bin/env python
import logging

from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from core.Configuration import Configuration
from core.GenericFile import GenericFile
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
        self.mkdir(path='/', mode=0o770000)

    """
        Create a file and returns a "file descriptor", which is in fact, simply the _id.
    """
    def create(self, path, mode):
        f = GenericFile.new_generic_file(filename=path, mode=mode, file_type=GenericFile.FILE_TYPE)
        return f.file_descriptor

    """
        Create a directory, no need to return anything
    """
    def mkdir(self, path, mode):
        print('Try to create '+path+' & '+str(mode))
        GenericFile.new_generic_file(filename=path, mode=mode, file_type=GenericFile.DIRECTORY_TYPE)

    """
        List files inside a directory
    """
    def readdir(self, path, fh):
        print('List directory for "'+str(path)+'" & "'+str(fh)+'"')
        files = self.mongo.list_files(directory=path)
        return ['.', '..'] + files

if __name__ == '__main__':
    if len(argv) < 2:
        print('usage: %s <mountpoint> (<configuration_filepath>)' % argv[0])
        exit(1)

    if len(argv) == 3:
        configuration_filepath = argv[2]
        Configuration.FILEPATH = configuration_filepath

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(MongoFS(), argv[1], foreground=True)