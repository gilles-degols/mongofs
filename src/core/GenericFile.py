import os
from stat import S_IFDIR, S_IFLNK, S_IFREG
import time

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
    Another interesting reading: https://www.gnu.org/software/libc/manual/html_node/Attribute-Meanings.html
"""
class GenericFile:
    # List of fields authorized for fusepy
    AUTHORIZED_METADATA_FIELDS = ['st_dev','st_ino','st_nlink','st_mode','st_uid','st_gid','st_rdev','st_size','st_blksize','st_blocks','st_atime','st_mtime','st_ctime']

    # Enum to easily detect the file type
    FILE_TYPE = 1
    DIRECTORY_TYPE = 2
    LINK_TYPE = 3

    # Link to the mongo instance, created at startup
    mongo = None

    def __init__(self, json):
        if not self.is_valid(json=json):
            raise ValueError("Invalid json received.")

        self.json = json
        self.file_descriptor = 0

    """
        Return an instance of a file / directory when we want to create a new one
    """
    @staticmethod
    def new_generic_file(filename, mode, file_type):
        directory = GenericFile.get_directory(filename=filename)

        if not GenericFile.is_generic_filename_available(filename=filename):
            print('GenericFile not available for '+filename+', we do nothing.')
            return None

        if filename != '/':
            GenericFile.mongo.add_nlink_directory(directory=directory, value=1)

        # Basic structure of the document to create in MongoDB
        dt = time.time()
        struct = {
            'directory': directory,
            'filename': filename,
            'file_type': file_type,
            'metadata': {
                'st_size': 0,
                'st_ctime': dt,
                'st_mtime': dt,
                'st_atime': dt
            }
        }

        if file_type == GenericFile.FILE_TYPE:
            struct['metadata']['st_nlink'] = 1
            struct['medatata']['st_mode'] = (S_IFREG | mode)
        elif file_type == GenericFile.DIRECTORY_TYPE:
            # If this is a directory, the default value st_nlink must be 2, to be sure to have a non-zero value if there is no
            # referenced file in the directory
            struct['metadata']['st_nlink'] = 2
            struct['metadata']['st_mode'] = (S_IFDIR | mode)
        else:
            print('Unsupported file type: '+str(file_type))
            exit(1)

        # We create the file, then ask Mongo to save it, and finally we return it.
        f = GenericFile(json=struct)
        GenericFile.mongo.create_generic_file(f)

        return f

    """
        Compute the directory for the given filename. Return a String only.
        For the "/" filename, it will return ""
    """
    @staticmethod
    def get_directory(filename):
        # Format the path
        if filename == '/':
            return ''

        if filename != '/' and filename.endswith('/'):
            print('A directory or a file cannot finish with a /.')
            return None

        directory = '/'.join(filename.split('/')[:-1])
        if not directory.startswith('/'):
            directory = '/' + directory

        return directory

    """
        Check if we can create a generic file for the given path
    """
    @staticmethod
    def is_generic_filename_available(filename):
        # We try to look for the directory directly above it (only one layer above), as we need to increase the "st_nlink" value.
        directory = GenericFile.get_directory(filename=filename)
        if filename != '/' and not GenericFile.mongo.generic_file_exists(filename=directory):
            print('Missing intermediate directory "'+directory+'" for file "'+filename+'".')
            return True

        # Now we need to verify there is no similar file already existing
        if GenericFile.mongo.get_generic_file(filename=filename) is not None:
            print('A file already exists with the path "' + filename + '".')
            return False

        return True

    """
        Verify the integrity of a json, is it a "file" in MongoFS?
    """
    @staticmethod
    def is_valid(json):
        if 'metadata' not in json or len(json['metadata']) == 0:
            print('Missing metadata information while trying to load the object: ' + str(json))
            return False

        fields_are_ok = True
        for field_name in json['metadata']:
            if field_name not in GenericFile.AUTHORIZED_METADATA_FIELDS:
                print('Invalid field ("'+str(field_name)+'") found in the file: '+str(json['filename']))
                fields_are_ok = False

        return fields_are_ok

    """
        Returns the current file metadata for the fuse api 
    """
    def to_fuse(self):
        return self.json['metadata']

