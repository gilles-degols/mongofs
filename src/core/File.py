import os

"""
    Interface between the MongoDB structure of a file and the file we need to provide to fusepy.
    It also contains various methods for common operations related to the files.
    List of (interesting) parameters for x86_64:
        ('st_dev', c_dev_t),
        ('st_ino', ctypes.c_ulong),
        ('st_nlink', ctypes.c_ulong),
        ('st_mode', c_mode_t),
        ('st_uid', c_uid_t),
        ('st_gid', c_gid_t),
        ('__pad0', ctypes.c_int),
        ('st_rdev', c_dev_t),
        ('st_size', c_off_t),
        ('st_blksize', ctypes.c_long),
        ('st_blocks', ctypes.c_long),
        ('st_atimespec', c_timespec),
        ('st_mtimespec', c_timespec),
        ('st_ctimespec', c_timespec)]
    Read https://github.com/fusepy/fusepy/blob/master/fuse.py for a list of all parameters
"""
class File:

    def __init__(self, json):
        self.json = json

    """
        Returns the current file metadata for the fuse api 
    """
    def to_fuse(self):
        return self.json['metadata']

