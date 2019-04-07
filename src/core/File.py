#!/usr/lib/mongofs/environment/bin/python
import errno

from fuse import FuseOSError

from src.core.GenericFile import GenericFile

class File(GenericFile):
    """
        Add data to the existing file (it might already have some)
    """
    def add_data(self, data, offset):
        if not GenericFile.has_user_access_right(self, GenericFile.WRITE_RIGHTS):
            raise FuseOSError(errno.EACCES)
        GenericFile.mongo.add_data(file=self, data=data, offset=offset)

    """
        Read a chunk of data from the file
    """
    def read_data(self, offset, size):
        if not GenericFile.has_user_access_right(self, GenericFile.READ_RIGHTS):
            raise FuseOSError(errno.EACCES)
        return GenericFile.mongo.read_data(file=self, offset=offset, size=size)

    """
        Truncate a file to a specific size
    """
    def truncate(self, length):
        if not GenericFile.has_user_access_right(self, GenericFile.WRITE_RIGHTS):
            raise FuseOSError(errno.EACCES)
        GenericFile.mongo.truncate(file=self, length=length)

    """
        Indicates if the current GenericFile is in fact a file
    """
    def is_file(self):
        return True
