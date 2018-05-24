#!/usr/bin/env python
import logging
import time

from sys import argv, exit
from errno import ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from core.Configuration import Configuration
from core.GenericFile import GenericFile
from core.File import File
from core.SymbolicLink import SymbolicLink
from core.Mongo import Mongo

"""
    Simulate a file system running on MongoDB.
    A simple example to start: https://github.com/terencehonles/fusepy/blob/master/examples/memory.py
    List of fuse low-level methods: https://pike.lysator.liu.se/generated/manual/modref/ex/predef_3A_3A/Fuse/Operations/
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
        file = GenericFile.new_generic_file(filename=path, mode=mode, file_type=GenericFile.FILE_TYPE)
        return file.file_descriptor

    """
        Create a symbolic link to a generic file (source -> target). No need to return a file descriptor.
         target: the file we want to display if we display "source"
         source: the symbolic link itself
        Important: It seems we receive the symlink parameters in a different order than "ln -s TARGET SYMLINK",
        we receive them (SYMLINK, TARGET) which is kinda weird.
    """
    def symlink(self, source, target):
        GenericFile.new_generic_file(filename=source, mode=0o777, file_type=GenericFile.SYMBOLIC_LINK_TYPE, target=target)

    """
        Read a symbolic link and return the file we should be redirected to.
    """
    def readlink(self, path):
        link = self.mongo.get_generic_file(filename=path)
        return link.get_target()

    """
        Read a part of a file
    """
    def read(self, path, size, offset, fh):
        file = self.mongo.get_generic_file(filename=path)
        return file.read_data(offset=offset, size=size)

    """
        Delete a file
    """
    def unlink(self, path):
        file_or_link = self.mongo.get_generic_file(filename=path)
        self.mongo.remove_generic_file(generic_file=file_or_link)

    """
        Create a directory, no need to return anything
    """
    def mkdir(self, path, mode):
        GenericFile.new_generic_file(filename=path, mode=mode, file_type=GenericFile.DIRECTORY_TYPE)

    """
        Delete a directory
    """
    def rmdir(self, path):
        directory = self.mongo.get_generic_file(filename=path)
        self.mongo.remove_generic_file(generic_file=directory)

    """
        List files inside a directory
    """
    def readdir(self, path, fh):
        files = self.mongo.list_generic_files_in_directory(directory=path)

        # We need to only keep final filename
        filenames = [file.filename.split('/')[-1] for file in files]
        return ['.', '..'] + filenames

    """
        Write data to a file, from a specific offset. Returns the written data size
    """
    def write(self, path, data, offset, fh):
        file = self.mongo.get_generic_file(filename=path)
        file.add_data(data=data, offset=offset)
        return len(data)

    """
        Truncate a file to a specific length
    """
    def truncate(self, path, length, fh=None):
        file = self.mongo.get_generic_file(filename=path)
        file.truncate(length=length)

    """
        Return general information for a given path
    """
    def getattr(self, path, fh=None):
        gf = self.mongo.get_generic_file(filename=path)
        if gf is None:
            raise FuseOSError(ENOENT)

        return gf.metadata

    """
        Set permissions to a given path
    """
    def chmod(self, path, mode):
        gf = self.mongo.get_generic_file(filename=path)
        gf.metadata['st_mode'] &= 0o770000
        gf.metadata['st_mode'] |= mode
        gf.basic_save()
        return 0

    """
        Set owner (user & group) to a given path
    """
    def chown(self, path, uid, gid):
        gf = self.mongo.get_generic_file(filename=path)
        gf.metadata['st_uid'] = uid
        gf.metadata['st_gid'] = gid
        gf.basic_save()

    """
        Return a specific special attribute for a given path (for selinux for example)
    """
    def getxattr(self, path, name, position=0):
        gf = self.mongo.get_generic_file(filename=path)
        try:
            return gf.attrs[name]
        except KeyError:
            return ''  # Should return ENOATTR

    """
        Return all special attributes for a given path (for selinux for example)
    """
    def listxattr(self, path):
        gf = self.mongo.get_generic_file(filename=path)
        return gf.attrs.keys()

    """
        Remove a specific special attribute for a given path
    """
    def removexattr(self, path, name):
        gf = self.mongo.get_generic_file(filename=path)

        if name in gf.attrs:
            del gf.attrs[name]
            gf.basic_save()
        else:
            # Should return ENOATTR
            pass

    """
        Update the access and update time for a given path
    """
    def utimens(self, path, times=None):
        now = time.time()
        atime, mtime = times if times else (now, now)
        gf = self.mongo.get_generic_file(filename=path)
        gf.metadata['st_atime'] = atime
        gf.metadata['st_mtime'] = mtime
        gf.basic_save()

    """
        Update a specific special attribute for a given path 
    """
    def setxattr(self, path, name, value, options, position=0):
        gf = self.mongo.get_generic_file(filename=path)
        gf.attrs[name] = value
        gf.basic_save()


if __name__ == '__main__':
    if len(argv) < 2:
        print('usage: %s <mountpoint> (<configuration_filepath>)' % argv[0])
        exit(1)

    if len(argv) == 3:
        configuration_filepath = argv[2]
        Configuration.FILEPATH = configuration_filepath

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(MongoFS(), argv[1], foreground=True)