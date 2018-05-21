#!/usr/bin/env python
import logging

from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from core.Configuration import Configuration
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