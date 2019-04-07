#!/usr/lib/mongofs/environment/bin/python

import logging
import time
import os
import errno
import fcntl
from ctypes import *

from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from src.core.Configuration import Configuration
from src.core.GenericFile import GenericFile
from src.core.Mongo import Mongo

"""
    Simulate a file system running on MongoDB.
    A simple example to start: https://github.com/terencehonles/fusepy/blob/master/examples/memory.py
    List of fuse low-level methods: https://pike.lysator.liu.se/generated/manual/modref/ex/predef_3A_3A/Fuse/Operations/
"""
class MongoFS(LoggingMixIn, Operations):
    # This is useful to be able to umount if there is an error to access MongoDB for example
    mounting_point = None

    def __init__(self):
        self.configuration = Configuration()
        self.mongo = Mongo()
        self.files = {}

        # Additional setup
        GenericFile.mongo = self.mongo
        GenericFile.configuration = self.configuration

        if self.configuration.is_development():
            # START DEBUG ONLY - Drop the old information from MongoDB
            self.mongo.clean_database()
            self.mongo = Mongo() # Create a new instance to be sure to have the top folder
            # END DEBUG ONLY

        # The top folder of the FS is automatically created by the Mongo class.
    """
        Create a file and returns a "file descriptor", which is in fact, simply the _id.
    """
    def create(self, path, mode, fi=None):
        file = GenericFile.new_generic_file(filepath=path, mode=mode, file_type=GenericFile.FILE_TYPE)
        return file.file_descriptor

    """
        Acquire a lock on a specific file.
    """
    def lock(self, path, fh, cmd, lock):
        # lock is a pointer to a struct flock. The first field is a short
        # with the lock type, so we can just cast it to a POINTER(c_short)
        # We won't manage locking only part of file, so we don't care about the
        # complete struct
        lock_type_pointer = cast(lock, POINTER(c_short))
        lock_type = lock_type_pointer[0]

        if cmd == fcntl.F_GETLK:
            blocking = test_lock_and_get_first_blocking(filepath=path, lock={'type': lock_type})
            if blocking is None:
                # We modify the pointer by setting F_UNLCK
                lock_type_pointer[0] = fcntl.F_UNLCK
            else:
                # We modify the pointer by setting the blocking type of the locks
                lock_type_pointer[0] = blocking['type']
        elif cmd == fcntl.F_SETLK or cmd == fcntl.F_SETLKW:
            if self.mongo.get_generic_file(filepath=path, lock={'type': lock_type, 'wait': cmd == fcntl.F_SETLKW}) is None:
                raise FuseOSError(errno.ENOENT)
        else:
            raise FuseOSError(errno.EBADF)
        return 0

    """
        Release the lock on a specific file.
    """
    def release(self, path, fh):
        gf = self.mongo.get_generic_file(filepath=path)
        gf.release(filepath=path)
        return 0

    """
        Release the lock on a specific directory.
    """
    def releasedir(self, path, fh):
        gf = self.mongo.get_generic_file(filepath=path)
        gf.release(filepath=path)
        return 0

    """
        Rename a generic file
    """
    def rename(self, old, new):
        generic_file = self.mongo.get_generic_file(filepath=old)
        generic_file.rename_to(initial_filepath=old, destination_filepath=new)

    """
        Create a symbolic link to a generic file (source -> target). No need to return a file descriptor.
         target: the file we want to display if we display "source"
         source: the symbolic link itself
        Important: It seems we receive the symlink parameters in a different order than "ln -s TARGET SYMLINK",
        we receive them (SYMLINK, TARGET) which is kinda weird.
    """
    def symlink(self, source, target):
        GenericFile.new_generic_file(filepath=source, mode=0o777, file_type=GenericFile.SYMBOLIC_LINK_TYPE, target=target)

    """
        Read a symbolic link and return the file we should be redirected to.
    """
    def readlink(self, path):
        link = self.mongo.get_generic_file(filepath=path)
        return link.get_target()

    """
        Read a part of a file
    """
    def read(self, path, size, offset, fh):
        file = self.mongo.get_generic_file(filepath=path)
        tmp =  file.read_data(offset=offset, size=size)
        return tmp

    """
        Delete a file
    """
    def unlink(self, path):
        file_or_link = self.mongo.get_generic_file(filepath=path)
        self.mongo.remove_generic_file(generic_file=file_or_link)

    """
        Create a directory, no need to return anything
    """
    def mkdir(self, path, mode):
        GenericFile.new_generic_file(filepath=path, mode=mode, file_type=GenericFile.DIRECTORY_TYPE)

    """
        Delete a directory
    """
    def rmdir(self, path):
        directory = self.mongo.get_generic_file(filepath=path)
        self.mongo.remove_generic_file(generic_file=directory)

    """
        List files inside a directory
    """
    def readdir(self, path, fh):
        files = self.mongo.list_generic_files_in_directory(filepath=path)

        # We need to only keep final filename
        filenames = [file.filename for file in files]
        return ['.', '..'] + filenames

    """
        Write data to a file, from a specific offset. Returns the written data size
    """
    def write(self, path, data, offset, fh):
        file = self.mongo.get_generic_file(filepath=path)
        file.add_data(data=data, offset=offset)
        return len(data)

    """
        Truncate a file to a specific length
    """
    def truncate(self, path, length, fh=None):
        file = self.mongo.get_generic_file(filepath=path)
        file.truncate(length=length)

    """
        Return general information for a given path
    """
    def getattr(self, path, fh=None):
        gf = self.mongo.get_generic_file(filepath=path)
        if gf is None:
            raise FuseOSError(errno.ENOENT)

        metadata = gf.metadata
        if gf.host != self.configuration.hostname():
            if metadata['st_uid'] != 0:
                uid = self.mongo.get_userid(gf.uname)
                if uid is not None:
                    metadata['st_uid'] = uid

            if metadata['st_gid'] != 0:
                gid = self.mongo.get_groupid(gf.gname)
                if gid is not None:
                    metadata['st_gid'] = gid
        return metadata

    """
        Set permissions to a given path
    """
    def chmod(self, path, mode):
        gf = self.mongo.get_generic_file(filepath=path)
        gf.metadata['st_mode'] &= 0o770000
        gf.metadata['st_mode'] |= mode
        gf.basic_save()
        return 0

    """
        Set owner (user & group) to a given path
    """
    def chown(self, path, uid, gid):
        gf = self.mongo.get_generic_file(filepath=path)
        gf.host = self.configuration.hostname()
        gf.metadata['st_uid'] = uid
        gf.metadata['st_gid'] = gid
        gf.uname = self.mongo.get_username(uid)
        gf.gname = self.mongo.get_groupname(gid)
        gf.basic_save()

    """
        Return a specific special attribute for a given path (for selinux for example)
    """
    def getxattr(self, path, name, position=0):
        gf = self.mongo.get_generic_file(filepath=path)
        try:
            return gf.attrs[name]
        except KeyError:
            return ''  # Should return ENOATTR

    """
        Return all special attributes for a given path (for selinux for example)
    """
    def listxattr(self, path):
        gf = self.mongo.get_generic_file(filepath=path)
        return gf.attrs.keys()

    """
        Remove a specific special attribute for a given path
    """
    def removexattr(self, path, name):
        gf = self.mongo.get_generic_file(filepath=path)

        if name in gf.attrs:
            del gf.attrs[name]
            gf.basic_save()
        else:
            raise FuseOSError(errno.ENOATTR)

    """
        Update the access and update time for a given path
    """
    def utimens(self, path, times=None):
        now = time.time()
        atime, mtime = times if times else (now, now)
        gf = self.mongo.get_generic_file(filepath=path)
        gf.metadata['st_atime'] = atime
        gf.metadata['st_mtime'] = mtime
        gf.basic_save()

    """
        Update a specific special attribute for a given path 
    """
    def setxattr(self, path, name, value, options, position=0):
        gf = self.mongo.get_generic_file(filepath=path)
        gf.attrs[name] = value
        gf.basic_save()

    """
        General (static) information about the current file system.
    """
    def statfs(self, path):
        print('Call statfs for '+str(path)+'...')
        return dict(f_bsize=65536*8, f_frsize=65536*8, f_blocks=65536*8, f_bavail=65536)

    """
        Flush data to MongoDB
    """
    def flush(self, path, fh):
        file = self.mongo.get_generic_file(filepath=path)
        self.mongo.flush_data_to_write(file=file)
        return None

if __name__ == '__main__':
    if len(argv) < 2:
        print('usage: %s <mountpoint> (<configuration_filepath>)' % argv[0])
        exit(1)

    if len(argv) == 3:
        configuration_filepath = argv[2]
        Configuration.FILEPATH = configuration_filepath

    mounting_point = str(argv[1])
    if not mounting_point.startswith('/'):
        mounting_point = os.getcwd() + '/' + mounting_point

    # If the previous mount failed, we need to be sure to umount it, otherwise you will not be able to mount anymore
    Configuration.mounting_point = mounting_point
    os.system('fusermount -u ' + mounting_point + ' &>/dev/null')

    allow_other = True
    if os.getuid() != 0:
        # If we are not mounting in root, we have to check option user_allow_other in /etc/fuse.conf
        try:
            file = open('/etc/fuse.conf', 'r')
            allow_other = 'user_allow_other' in list(map(lambda l: l.strip(), file.readlines()))
            file.close()
        except:
            allow_other = False

    configuration = Configuration()
    if configuration.is_development():
        logging.basicConfig(level=logging.DEBUG)
        fuse = FUSE(MongoFS(), mounting_point, foreground=True, nothreads=True, allow_other=allow_other)
    else:
        logging.basicConfig(level=logging.ERROR)
        fuse = FUSE(MongoFS(), mounting_point, foreground=False, nothreads=False, allow_other=allow_other)
