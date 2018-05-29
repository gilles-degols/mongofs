#!/usr/bin/env python
import errno
from math import floor, ceil
from errno import ENOENT, EDEADLOCK
import time
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

from pymongo import MongoClient
import gridfs
from pymongo.collection import ReturnDocument

from src.core.Configuration import Configuration
from src.core.GenericFile import GenericFile
from src.core.File import File
from src.core.Directory import Directory
from src.core.SymbolicLink import SymbolicLink

class Mongo:
    instance = None
    configuration = None

    LOCKED_FILE = 1
    FILE_NOT_FOUND = 2

    def __init__(self):
        # We reuse the same connexion
        if Mongo.instance is None:
            Mongo.configuration = Configuration()
            self.connect()
        self.instance = Mongo.instance
        self.database = Mongo.instance[Mongo.configuration.mongo_database()]

        # We use gridfs only to store the files. Even if we have a lot of small files, the overhead should
        # still be small.
        # Documentation: https://api.mongodb.com/python/current/api/gridfs/index.html
        self.gridfs_collection = Mongo.configuration.mongo_prefix() + 'files'
        self.gridfs = gridfs.GridFS(self.database, self.gridfs_collection)

        self.files_coll =  self.database[Mongo.configuration.mongo_prefix() + 'files.files']
        self.chunks_coll =  self.database[Mongo.configuration.mongo_prefix() + 'files.chunks']

        # We need to be sure to have the top folder created in MongoDB
        GenericFile.mongo = self
        GenericFile.new_generic_file(filepath='/', mode=0o755, file_type=GenericFile.DIRECTORY_TYPE)

    """
        Establish a connection to mongodb
    """
    def connect(self):
        mongo_path = 'mongodb://' + ','.join(Mongo.configuration.mongo_hosts())
        Mongo.instance = MongoClient(mongo_path)

    """
        Load the appropriate object for the given json. Should never return a GenericFile, but rather a child class.
    """
    @staticmethod
    def load_generic_file(json):
        if json['generic_file_type'] == GenericFile.FILE_TYPE:
            return File(json)
        elif json['generic_file_type'] == GenericFile.DIRECTORY_TYPE:
            return Directory(json)
        elif json['generic_file_type'] == GenericFile.SYMBOLIC_LINK_TYPE:
            return SymbolicLink(json)
        else:
            print('Unsupported file type!')
            return GenericFile(json)

    """
        Some information about the current user.
    """
    @staticmethod
    def current_user():
        raw = fuse_get_context()
        return {'uid':raw[0],'gid':raw[1],'pid':raw[2]}

    """
        Give the appropriate lock id containing:
            filename
            pid
            hostname            
    """
    @staticmethod
    def lock_id(filepath):
        lock_id = filepath + ';' + str(Mongo.current_user()['pid']) + ';' + str(Mongo.configuration.hostname())
        return lock_id

    """
        Give the master lock id containing:
            filename
            pid -> 0
            hostname
        This "master" lock id is used to unlock specific locked file, as we sometimes receive an unlock order with the 
        PID 0, and we need to fulfill it anyway
    """
    @staticmethod
    def master_lock_id(filepath):
        lock_id = filepath + ';' + str(0) + ';' + str(Mongo.configuration.hostname())
        return lock_id

    """
        Create a generic file in gridfs. No need to return it.
    """
    def create_generic_file(self, generic_file):
        f = self.gridfs.new_file(**generic_file.json)
        f.close()

    """
        Remove a generic file. No need to verify if the file already exists, the check is done by FUSE.
    """
    def remove_generic_file(self, generic_file):
        # We cannot directly remove every sub-file in the directory (permissions check to do, ...), but we need to
        # be sure the directory is empty.
        if generic_file.is_dir():
            if self.files_coll.find({'directory_id':generic_file._id}).count() != 0:
                raise FuseOSError(errno.ENOTEMPTY)

        # First we delete the file (metadata + chunks)
        self.gridfs.delete(generic_file._id)

        # Then we decrease the number of link in the directory above it
        self.add_nlink_directory(directory_id=generic_file.directory_id, value=-1)

    """
        List files in a given directory. 
        TODO: Change the method signature maybe?
    """
    def list_generic_files_in_directory(self, filepath):
        dir = self.get_generic_file(filepath=filepath)
        files = []
        print('Try to list files matching directory_id: '+str(dir._id))
        for elem in self.files_coll.find({'directory_id':dir._id}, no_cursor_timeout=True):
            files.append(Mongo.load_generic_file(elem))
        return files

    """
        Indicate if the generic file exists or not. 
    """
    def generic_file_exists(self, filepath):
        return self.get_generic_file(filepath=filepath) is not None

    """
        Wrapper around get_generic_file_internal() in charge of waiting x seconds before throwing a "LOCKED_FILE" exception 
        to the user.
        Return a 
         GenericFile instance, if there is one matching file available
         Throw an error if there is lock
         None if the file is not found
    """
    def get_generic_file(self, filepath, take_lock=False):
        dt = time.time()
        lock_max_access = Mongo.configuration.lock_access_attempt()
        gf = Mongo.LOCKED_FILE

        if take_lock is True:
            lock_id = Mongo.lock_id(filepath=filepath)
        else:
            lock_id = None

        directory_id = self.get_last_directory_id_for_filepath(filepath=filepath)
        filename = filepath.split('/')[-1]
        while gf == Mongo.LOCKED_FILE and dt + lock_max_access >= time.time():
            gf = self.get_generic_file_internal(filepath=filepath, directory_id=directory_id, filename=filename, lock=lock_id)
            if gf == Mongo.LOCKED_FILE:
                # Wait 1s before checking the lock again
                time.sleep(1)
            elif gf == Mongo.FILE_NOT_FOUND:
                return None
            else:
                return gf

        # It means the file is still locked
        raise FuseOSError(EDEADLOCK)

    """
        Retrieve any file / directory / link document from Mongo. 
        If "lock" contains a lock id (versus None), we only return the generic file if we were able to put the lock on it directly.
        We do not analyze the intermediate locks (in the directories) as this will be done by FUSE directly.
        Be careful: If one process did not put a lock, and another wants to add it, it will be able to do it obviously.
        Returns 
         GenericFile instance (child of that class in fact)
         Mongo.LOCKED_FILE if the file is currently locked (we tried to access it multiple times during a short amount of time before returning that information)
         Mongo.FILE_NOT_FOUND None if none are found.
    """
    def get_generic_file_internal(self, filepath, directory_id, filename, lock=None):
        dt = time.time()
        dt_timeout = dt + Mongo.configuration.lock_timeout()

        # The same process could be the one having created the lock, so we need to let him have access to the file
        current_process_lock = Mongo.lock_id(filepath=filepath)
        master_process_lock = Mongo.master_lock_id(filepath=filepath)
        if lock is not None:
            gf = self.files_coll.find_one_and_update({'directory_id':directory_id,'filename': filename,'lock':{'$exists':False}},
                                                     {'$set':{'lock':{'creation':dt,'id':lock}}},
                                                    return_document=ReturnDocument.AFTER)
            if gf is None:
                # File might not exist, we don't know for sure.
                gf = self.files_coll.find_one({'directory_id':directory_id,'filename': filename})
                if gf is None:
                    return Mongo.FILE_NOT_FOUND
                elif gf['lock']['creation'] >= dt_timeout:
                    diff = int(dt - gf['lock']['creation'])
                    print('Lock for '+filename+' was created/updated '+str(diff)+'s ago. Maximum timeout is '+
                          str(Mongo.configuration.lock_timeout()+', so we remove the lock (if it was not taken by another '+
                          'process right now).'))
                    self.files_coll.find_one_and_update({'directory_id':directory_id, 'filename': filename, 'lock.id':gf['lock']['id']},
                                                          {'$unset':{'lock':''}}, return_document=ReturnDocument.AFTER)
                    return self.get_generic_file_internal(filename=filename, lock=lock)
                elif gf['lock']['id'] == current_process_lock:
                    # It means it was the same that acquired the log, so he can have access to the file
                    return Mongo.load_generic_file(gf)
                else:
                    return Mongo.LOCKED_FILE
            return Mongo.load_generic_file(gf)
        else:
            # We don't really need to verify the lock directly in the MongoDB query, the logic will be outside MongoDB anyway
            gf = self.files_coll.find_one({'directory_id':directory_id,'filename': filename})
            if gf is not None:
                if 'lock' in gf and gf['lock']['id'] != current_process_lock and current_process_lock != master_process_lock:
                    return Mongo.LOCKED_FILE
                else:
                    return Mongo.load_generic_file(gf)
            return Mongo.FILE_NOT_FOUND

    """
        Return the last directory_id for a given filepath. Return None if none are found.
    """
    def get_last_directory_id_for_filepath(self, filepath, previous_directory_id=None):
        if filepath == '/': # Exception for '/' path as it generates a ['',''] list.
            elems = ['']
        else:
            elems = filepath.split('/')

        first_directory_name = elems[0]
        if len(elems) == 1:
            # We are at the last iteration, so in reality we would be testing the filename itself (not the goal),
            # we can directly return the directory_id
            return previous_directory_id

        # We should ideally check on the generic_file_type to be a DIR, not sure though are handled the symbolic link...
        # Did we receive directly the path correctly redirected? Or did we receive the path with the symbolic link in it? TODO: Verify it.
        dir = self.files_coll.find_one({'directory_id':previous_directory_id,'filename':first_directory_name,'generic_file_type': GenericFile.DIRECTORY_TYPE})
        if dir is not None:
            return self.get_last_directory_id_for_filepath(filepath='/'.join(elems[1:]), previous_directory_id=dir['_id'])

        return None

    """
        Increment/reduce the number of links for a directory 
    """
    def add_nlink_directory(self, directory_id, value):
        # You cannot update directly the object from gridfs, you need to do a MongoDB query instead
        self.files_coll.find_one_and_update({'_id':directory_id},
                                                         {'$inc':{'metadata.st_nlink':value}})


    """
        Read data from a file 
         file: Instance of a "File" type object.
         offset: Offset from which we want to read the file
         length: Number of bytes we need to send back
        Return bytes array
    """
    def read_data(self, file, offset, size):
        # We get the chunks we are interested in
        chunk_size = file.chunkSize
        starting_chunk = int(floor(offset / chunk_size))
        ending_chunk = int(floor((offset + size) / chunk_size))

        starting_offset = offset % chunk_size
        ending_size = chunk_size
        data = b''
        for chunk in self.chunks_coll.find({'files_id':file._id,'n':{'$gte':starting_chunk,'$lte':ending_chunk}}):
            if chunk['n'] == ending_chunk:
                ending_size = size % chunk_size
            data += chunk['data'][starting_offset:ending_size]
            starting_offset = 0
        return data

    """
        Add data to a file. 
         file: Instance of a "File" type object.
         data: bytes 
    """
    def add_data(self, file, data, offset):
        # Normally, we should not update a gridfs document, but re-write everything. I don't see any specific reason
        # to do that, so we will try to update it anyway. But we will only rewrite the last chunks of it, or add information
        # to them, while keeping the limitation of ~255KB/chunk

        # Final size after the update
        total_size = offset + len(data)

        # Important note: the data that we receive are replacing any existing data from "offset".
        chunk_size = file.chunkSize
        total_chunks = int(ceil(file.length / chunk_size))
        starting_chunk = int(floor(offset / chunk_size))
        starting_byte = offset - starting_chunk * chunk_size
        if starting_byte < 0:
            print('Computation error for offset: '+str(offset))
        for chunk in self.chunks_coll.find({'files_id':file._id,'n':{'$gte':starting_chunk}}):
            chunk['data'] = chunk['data'][0:starting_byte] + data[0:chunk_size-starting_byte]
            self.chunks_coll.find_one_and_update({'_id':chunk['_id']},{'$set':{'data':chunk['data']}})

            # We have written a part of what we wanted, we only need to keep the remaining
            data = data[chunk_size-starting_byte:]

            # For the next chunks, we start to replace bytes from zero.
            starting_byte = 0

            # We might not need to go further to write the data
            if len(data) == 0:
                break

        # The code above was only to update a document, we might want to add new chunks
        if len(data) > 0:
            remaining_chunks = int(ceil(len(data) / chunk_size))
            for i in range(0, remaining_chunks):
                chunk = {
                    "files_id": file._id,
                    "data": data[0:chunk_size],
                    "n": total_chunks
                }
                self.chunks_coll.insert_one(chunk)

                # We have written a part of what we wanted, we only the keep the remaining
                data = data[chunk_size:]

                # Next entry
                total_chunks += 1

        # We update the total length and that's it
        self.files_coll.find_one_and_update({'_id':file._id},{'$set':{'length':total_size,'metadata.st_size':total_size}})

        return True

    """
        Truncate a part of a file 
         file: Instance of a "File" type object.
         length: Offset from which we need to truncate the file 
    """
    def truncate(self, file, length):
        # We drop every unnecessary chunk
        chunk_size = file.chunkSize
        maximum_chunks = int(ceil(length / chunk_size))
        self.chunks_coll.delete_many({'files_id':file._id,'n':{'$gte':maximum_chunks}})

        # We update the last chunk
        if length % chunk_size != 0:
            last_chunk = self.chunks_coll.find_one({'files_id':file._id,'n':maximum_chunks-1})
            last_chunk['data'] = last_chunk['data'][0:length % chunk_size]
            self.chunks_coll.find_one_and_update({'_id':last_chunk['_id']},{'$set':{'data':last_chunk['data']}})

        # We update the total length and that's it
        self.files_coll.find_one_and_update({'_id':file._id},{'$set':{'length':length,'metadata.st_size':length}})
        return True

    """
        Rename a generic file to another name
         generic_file: Instance of a GenericFile type object
         destination_filename: Expected filename
    """
    def rename_generic_file_to(self, generic_file, initial_filepath, destination_filepath):
        # There is no need to verify if the destination directory exists, and if there is not already a file with the same name,
        # as FUSE will automatically verify those conditions before calling our implementation of file moving.

        # First we decrease the number of nlink in the directory above (even if we might stay in the same repository
        # at the end, that's not a big deal)
        initial_directory_id = GenericFile.get_directory_id(filepath=initial_filepath)
        self.add_nlink_directory(directory_id=initial_directory_id, value=-1)

        # We rename it
        destination_directory_id = GenericFile.get_directory_id(filepath=destination_filepath)
        dest_filename = destination_filepath.split('/')[-1]
        self.files_coll.find_one_and_update({'_id':generic_file._id},{'$set':{'directory_id':destination_directory_id,'filename':dest_filename}})

        # We increase the number of nlink in the final directory
        self.add_nlink_directory(directory_id=destination_directory_id, value=-1)

    """
        Remove a lock to a generic file (only if we are owner of it). We do not care about the lock type. 
    """
    def unlock_generic_file(self, filepath, generic_file):
        lock_id = Mongo.lock_id(filepath=filepath)
        master_lock_id = Mongo.master_lock_id(filepath=filepath)
        if 'id' not in generic_file.lock or lock_id not in [master_lock_id, generic_file.lock['id']]:
            print('Trying to release a non-existing lock, or one that we do not own. We do nothing.')
            return True

        # For now, if we have a master-lock, we need to remove any lock. In that case, we should still verify that the lock
        # is released by the appropriate host (TODO).
        if lock_id == master_lock_id:
            self.files_coll.find_one_and_update({'_id': generic_file._id},
                                                {'$unset': {'lock': ''}}, return_document=ReturnDocument.AFTER)
        else:
            self.files_coll.find_one_and_update({'_id': generic_file._id, 'lock.id': lock_id},
                                                {'$unset': {'lock': ''}}, return_document=ReturnDocument.AFTER)
        return True

    """
        Update some arbitrary fields in the general "files" object
    """
    def basic_save(self, generic_file, metadata, attrs):
        self.files_coll.find_one_and_update({'_id':generic_file._id},{'$set':{'metadata':metadata,'attrs':attrs}})

    """
        Clean the database, only for development purposes
    """
    def clean_database(self):
        self.chunks_coll.drop()
        self.files_coll.drop()