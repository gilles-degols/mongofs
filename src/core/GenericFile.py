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
    Available errors: https://docs.python.org/3/library/errno.html
"""
class GenericFile:
    # Enum to easily detect the file type
    FILE_TYPE = 1
    DIRECTORY_TYPE = 2
    SYMBOLIC_LINK_TYPE = 3

    # Link to the mongo instance, created at startup
    mongo = None

    """
        We can create a GenericFile instance from a raw json only.
    """
    def __init__(self, json=None):
        self.json = json
        self.file_descriptor = 0

        self._id = json.get('_id', None)
        self.filename = json['filename']
        self.chunkSize = json.get('chunkSize', None)
        self.directory_id = json['directory_id']
        self.generic_file_type = json['generic_file_type']
        self.metadata = json['metadata']
        self.attrs = json.get('attrs',{})
        self.lock = json.get('lock',{})
        self.length = json['length']

    """
        Save specific fields modification of the current object to MongoDB. We do not allow
        the developer to directly update the "filename" or "chunkSize" for example. We only
        allow the update of a few fields (metadata for example).
        This method will not work for a new generic-file, it must be created by new_generic_file.
    """
    def basic_save(self):
        GenericFile.mongo.basic_save(generic_file=self, metadata=self.metadata, attrs=self.attrs)

    """
        Indicates if the current GenericFile is in fact a directory
    """
    def is_dir(self):
        return False

    """
        Indicates if the current GenericFile is in fact a file
    """
    def is_file(self):
        return False

    """
        Indicates if the current GenericFile is in fact a link
    """
    def is_link(self):
        return False

    """
        Rename a generic file to another filepath
    """
    def rename_to(self, filepath):
        GenericFile.mongo.rename_generic_file_to(generic_file=self, destination_filepath=filepath)
        # We update the related filename and directory
        self.filename = filepath.split('/')[-1]
        self.directory_id = GenericFile.get_directory_id(filepath=filepath)

    """
        Try to release a lock on a generic file. The lock is initially generated when we try to load the file, so there
        is no method here for the opposite management of lock. 
    """
    def unlock(self, filepath):
        return GenericFile.mongo.unlock_generic_file(filepath=filepath, generic_file=self)

    """
        Return an instance of a file / directory when we want to create a new one
    """
    @staticmethod
    def new_generic_file(filepath, mode, file_type, target=None):
        directory_id = GenericFile.get_directory_id(filepath=filepath)

        if not GenericFile.is_generic_filepath_available(filepath=filepath):
            print('GenericFile not available for '+filepath+', we do nothing.')
            return None

        if filepath != '/':
            GenericFile.mongo.add_nlink_directory(directory_id=directory_id, value=1)

        # Basic structure of the document to create in MongoDB
        filename = filepath.split('/')[-1]
        dt = time.time()
        struct = {
            'directory_id': directory_id,
            'filename': filename,
            'generic_file_type': file_type,
            'metadata': {
                'st_size': 0,
                'st_ctime': dt,
                'st_mtime': dt,
                'st_atime': dt
            },
            'length': 0
        }

        if file_type == GenericFile.FILE_TYPE:
            struct['metadata']['st_nlink'] = 1
            struct['metadata']['st_mode'] = (S_IFREG | mode)
        elif file_type == GenericFile.DIRECTORY_TYPE:
            # If this is a directory, the default value st_nlink must be 2, to be sure to have a non-zero value if there is no
            # referenced file in the directory
            struct['metadata']['st_nlink'] = 2
            struct['metadata']['st_mode'] = (S_IFDIR | mode)
        elif file_type == GenericFile.SYMBOLIC_LINK_TYPE:
            struct['metadata']['st_nlink'] = 1
            struct['metadata']['st_mode'] = (S_IFLNK | mode)
            struct['metadata']['st_size'] = len(filename)
            struct['length'] = len(filename)
            struct['target'] = target
        else:
            print('Unsupported file type: '+str(file_type))
            exit(1)

        # We create the file, then ask Mongo to save it, and finally we return it.
        f = GenericFile(json=struct)
        GenericFile.mongo.create_generic_file(f)

        return f

    """
        Compute the directory for the given filepath. Return a String only.
        For the "/" filename, it will return ""
    """
    @staticmethod
    def get_directory_name(filepath):
        # Format the path
        if filepath == '/':
            return ''

        if filepath != '/' and filepath.endswith('/'):
            print('A directory or a file cannot finish with a /.')
            return None

        directory = '/'.join(filepath.split('/')[:-1])
        if not directory.startswith('/'):
            directory = '/' + directory

        return directory

    """
        Get the directory _id for the given filepath. Return a {$oid: "...."}.
        For the "/" filename, it will return None
    """
    @staticmethod
    def get_directory_id(filepath):
        directory = GenericFile.get_directory_name(filepath=filepath)
        dir = GenericFile.mongo.get_generic_file(filepath=directory)
        if dir is None:
            return None
        return dir._id

    """
        Check if we can create a generic file for the given path
    """
    @staticmethod
    def is_generic_filepath_available(filepath):
        # We try to look for the directory directly above it (only one layer above), as we need to increase the "st_nlink" value.
        directory_name = GenericFile.get_directory_name(filepath=filepath)
        if filepath != '/' and not GenericFile.mongo.generic_file_exists(filepath=directory_name):
            # There is no need to verify if this level above the current file is a directory or not, it will be automatically
            # checked by FUSE with readdir()
            print('Missing intermediate directory "'+directory_name+'" for file "'+filepath+'".')
            return True

        # Now we need to verify there is no similar file already existing
        if GenericFile.mongo.get_generic_file(filepath=filepath) is not None:
            print('A file already exists with the path "' + filepath + '".')
            return False

        return True
