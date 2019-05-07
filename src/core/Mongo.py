#!/usr/lib/mongofs/environment/bin/python
import errno
import grp
import pwd
import os
from math import floor, ceil
import time
import pymongo
import logging
from expiringdict import ExpiringDict
from fuse import FuseOSError, fuse_get_context
from stat import S_IFDIR

from src.core.Configuration import Configuration
from src.core.MongoCache import MongoCache
from src.core.GenericFile import GenericFile
from src.core.File import File
from src.core.Directory import Directory
from src.core.SymbolicLink import SymbolicLink

class Mongo:
    cache = None
    configuration = None
    logger = logging.getLogger('Mongo')

    LOCKED_FILE = 1
    FILE_NOT_FOUND = 2

    # The do_clean_up argument is useful if we want to remove all entries from the db without taking care on wrong data in it (useful for test)
    def __init__(self, do_clean_up = False):
        # We reuse the same connexion
        Mongo.configuration = Configuration()
        Mongo.cache = MongoCache()

        # Collection name
        self.gridfs_coll = Mongo.configuration.mongo_prefix() + 'files'
        self.files_coll = Mongo.configuration.mongo_prefix() + 'files.files'
        self.chunks_coll = Mongo.configuration.mongo_prefix() + 'files.chunks'

        if do_clean_up is True:
            self.clean_database()

        # Create the initial indexes
        self.create_indexes()

        # Temporary cache for the file data
        self.data_cache = {}

        # Temporary cache for user information
        self.user_cache = ExpiringDict(max_len=1000, max_age_seconds=2)

        # Temporary cache for group information
        self.group_cache = ExpiringDict(max_len=1000, max_age_seconds=2)

        # We need to be sure to have the top folder created in MongoDB
        GenericFile.mongo = self
        root = self.get_generic_file(filepath='/')
        default_root_mode = S_IFDIR | self.configuration.default_root_mode()
        if root is None:
            GenericFile.new_generic_file(filepath='/', mode=default_root_mode, file_type=GenericFile.DIRECTORY_TYPE)
        elif self.configuration.force_root_mode() and root.metadata['st_mode'] != default_root_mode:
            root.metadata['st_mode'] = default_root_mode
            root.basic_save()
        

    """
        Create various indexes if they do not exist. Only called at startup
    """
    def create_indexes(self):
        Mongo.cache.create_index(self.files_coll, [("directory_id", pymongo.ASCENDING), ("filename", pymongo.ASCENDING)])
        # Gridfs drivers will automatically add {filename:1, uploadDate:1} as index too, see: https://docs.mongodb.com/manual/core/gridfs/#the-files-index


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
    def current_user(self):
        raw = fuse_get_context()
        return self.user(raw[0], raw[1], raw[2])        

    """
        Some information about the user running the process.
    """
    def process_user(self):
        return self.user(os.getuid(), os.getgid(), os.getpid())

    """
        Some information about the user with uid and gid
    """
    def user(self, uid, gid, pid):
        try:
            user_info = self.user_cache[uid]
        except KeyError:
            pw_uid = pwd.getpwuid(uid)
            gids = [g.gr_gid for g in grp.getgrall() if pw_uid.pw_name in g.gr_mem or pw_uid.pw_name == g.gr_name]
            gnames = [g.gr_name for g in grp.getgrall() if pw_uid.pw_name in g.gr_mem or pw_uid.pw_name == g.gr_name]

            try:
                gids.index(gid)
            except ValueError:
                gids.append(gid)
            user_info = {'uname': pw_uid.pw_name, 'gids': gids, 'gnames': gnames}
            self.user_cache[uid] = user_info
        return {'uid': uid, 'gid': gid, 'pid': pid, 'uname': user_info['uname'], 'gids': user_info['gids'], 'gnames': user_info['gnames']}

    """
        Get user name name for a name id
    """
    def get_username(self, uid):
        try:
           return self.user_cache[uid]['uname']
        except KeyError:
            try:
                return pwd.getpwuid(uid).pw_name
            except KeyError:
                return None


    """
        Get user name name for a name id
    """
    def get_userid(self, uname):
        for uid, val in self.user_cache.items():
            if val['uname'] == uname:
                return uid
        try:
            return pwd.getpwnam(uname).pw_uid
        except KeyError:
            return None

    """
        Get group name for a group id
    """
    def get_groupname(self, gid):
        try:
             return self.group_cache[gid]
        except KeyError:
            try:
                gname = grp.getgrgid(gid).gr_name
                self.group_cache[gid] = gname
                return gname
            except KeyError:
                return None

    """
        Get group id for a group name
    """
    def get_groupid(self, gname):
        for gid, name in self.group_cache.items():
            if name == gname:
                return gid

        try:
            gid = grp.getgrnam(gname).gr_gid
            self.group_cache[gid] = gname
            return gid
        except KeyError:
            return None

    """
        Give the appropriate lock id containing:
            filename
            pid
            hostname            
    """
    def lock_id(self, filepath):
        lock_id = filepath + ';' + str(self.current_user()['pid']) + ';' + str(Mongo.configuration.hostname())
        return lock_id

    """
        Create a generic file in gridfs. No need to return it.
    """
    def create_generic_file(self, generic_file):
        Mongo.cache.gridfs_new_file(generic_file.json)

    """
        Remove a generic file. No need to verify if the file already exists, the check is done by FUSE.
    """
    def remove_generic_file(self, generic_file):
        if not GenericFile.has_user_access_right(generic_file, GenericFile.WRITE_RIGHTS):
            raise FuseOSError(errno.EACCES)

        # We cannot directly remove every sub-file in the directory (permissions check to do, ...), but we need to
        # be sure the directory is empty.
        if generic_file.is_dir():
            if Mongo.cache.find(self.files_coll, {'directory_id': generic_file._id}).count() != 0:
                raise FuseOSError(errno.ENOTEMPTY)

        # First we delete the file (metadata + chunks)
        Mongo.cache.gridfs_delete(generic_file._id)

        # Then we decrease the number of link in the directory above it
        self.add_nlink_directory(directory_id=generic_file.directory_id, value=-1)

    """
        List files in a given directory. 
        TODO: Change the method signature maybe?
    """
    def list_generic_files_in_directory(self, filepath):
        dir = self.get_generic_file(filepath=filepath)
        if not GenericFile.has_user_access_right(dir, GenericFile.EXECUTE_RIGHTS):
            raise FuseOSError(errno.EACCES)

        files = []
        for elem in Mongo.cache.find(self.files_coll, {'directory_id':dir._id}):
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
    def get_generic_file(self, filepath, lock=None):
        dt = time.time()
        lock_max_access = Mongo.configuration.lock_access_attempt()
        gf = Mongo.LOCKED_FILE

        if lock is not None:
            lock['id'] = self.lock_id(filepath=filepath)

        directory_id = self.get_last_directory_id_for_filepath(filepath=filepath)
        filename = filepath.split('/')[-1]
        while gf == Mongo.LOCKED_FILE and dt + lock_max_access >= time.time():
            gf = self.get_generic_file_internal(filepath=filepath, directory_id=directory_id, filename=filename, lock=lock)
            if gf == Mongo.LOCKED_FILE:
                if not 'wait' in lock or not lock['wait']:
                    break
                # Wait 1s before checking the lock again
                time.sleep(1)
            elif gf == Mongo.FILE_NOT_FOUND:
                return None
            else:
                return gf

        # It means the file is still locked
        raise FuseOSError(errno.EAGAIN)

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

        if lock is not None:
            gf = Mongo.cache.find_one(self.files_coll, {'directory_id': directory_id, 'filename': filename})
            if gf is None:
                return Mongo.FILE_NOT_FOUND
            else:
                self.logger.debug('set lock ' + ('unlock' if lock['type'] == GenericFile.LOCK_UNLOCK else ('read' if lock['type'] == GenericFile.LOCK_READ else 'write')))
                if 'lock' not in gf or ('lock' in gf and len(gf['lock'])) == 0:
                    if lock['type'] == GenericFile.LOCK_UNLOCK:
                        return Mongo.load_generic_file(gf)
                    else:
                        self.logger.debug('no lock, so set own lock')
                        Mongo.cache.find_one_and_update(self.files_coll,
                            {'$and': [
                                {'directory_id':directory_id,'filename': filename},
                                {'$or': [{'lock': {'$exists': False}}, {'lock': {'$size': 0}}]},
                            ]},
                            {'$set':{'lock_version': 1, 'lock':[{'creation':dt,'id':lock['id'],'type':lock['type'],'hostname':str(Mongo.configuration.hostname())}]}})
                else:
                    locks = list(filter(lambda l: l['creation'] < dt_timeout, gf['lock']))
                    if len(locks) == 0:
                        lock_ids = list(map(lambda l: l['id'], gf['lock']))
                        self.logger.debug('Lock fors '+filename+' reached maximum timeout is '+
                            str(Mongo.configuration.lock_timeout()+', so we remove the locks (if it was not taken by another '+
                            'process right now).'))
                        Mongo.cache.find_one_and_update(self.files_coll,
                                                        {'directory_id':directory_id, 'filename': filename, 'lock_version': gf['lock_version']},
                                                        {'$unset':{'lock':'', 'lock_version': ''}})
                    else:
                        only_own_lock = len(locks) == 1 and locks[0]['id'] == lock['id']
                        own_lock_present = len(list(filter(lambda l: l['id'] == lock['id'], locks))) > 0
                        if lock['type'] == GenericFile.LOCK_UNLOCK:
                            # unlock lock can be set only when you are the only one locking or when you belong a lock
                            if only_own_lock:
                                self.logger.debug('remove only personal own lock')
                                Mongo.cache.find_one_and_update(self.files_coll,
                                    {'directory_id':directory_id, 'filename': filename, 'lock_version': gf['lock_version']},
                                    {'$unset':{'lock':'', 'lock_version': ''}})
                            elif own_lock_present:
                                self.logger.debug('remove personal own lock amongst others')
                                Mongo.cache.find_one_and_update(self.files_coll,
                                    {'directory_id':directory_id, 'filename': filename},
                                    {'$pull':{'lock': {'id': lock['id']}}, '$inc': {'lock_version': 1}})
                                # here we need to return immediately instead of trying to call the function back
                                # otherwise we would end attempt to unlock a file with other locks
                                gf = Mongo.cache.find_one(self.files_coll, {'directory_id': directory_id, 'filename': filename})
                                return Mongo.load_generic_file(gf)
                            else:
                                return Mongo.LOCKED_FILE
                        elif only_own_lock:
                            # if we are the only one locking, we can just update our own lock
                            if locks[0]['type'] == lock['type']:
                                self.logger.debug('lock already set with right value')
                                return Mongo.load_generic_file(gf)
                            else:
                                self.logger.debug('update own only lock')
                                result = Mongo.cache.find_one_and_update(self.files_coll,
                                    {'directory_id':directory_id, 'filename': filename, 'lock_version': gf['lock_version']},
                                    {
                                        '$set':{'lock': [{'creation':dt,'id':lock['id'],'type':lock['type'],'hostname':str(Mongo.configuration.hostname())}]},
                                        '$inc': {'lock_version': 1}
                                    })
                        elif lock['type'] == GenericFile.LOCK_SHARED and locks[0]['type'] == GenericFile.LOCK_SHARED:
                            # if we want a shared lock, we can add one of all other locks are also shared
                            if own_lock_present:
                                self.logger.debug('own read lock present')
                                return Mongo.load_generic_file(gf)
                            else:
                                Mongo.cache.find_one_and_update(self.files_coll,
                                    {'directory_id':directory_id, 'filename': filename, 'lock_version': gf['lock_version']},
                                    {
                                        '$push':{'lock': {'creation':dt,'id':lock['id'],'type':lock['type'],'hostname':str(Mongo.configuration.hostname())}},
                                        '$inc': {'lock_version': 1}
                                    })
                        else:
                            # if we fall here, that means that either we try to set an exclusive lock and there are other locks presents
                            # or that we try to set a shared lock and the other lock is an exclusive one
                            return Mongo.LOCKED_FILE
            
            self.logger.debug('check lock applied')
            return self.get_generic_file_internal(filepath=filepath, directory_id=directory_id, filename=filename, lock=lock)
        else:
            # We don't really need to verify the lock here because the processes always ask an unlock or a lock before any operation
            gf = Mongo.cache.find_one(self.files_coll, {'directory_id':directory_id,'filename': filename})
            if gf is not None:
                return Mongo.load_generic_file(gf)
            return Mongo.FILE_NOT_FOUND

    """
        Test a lock and returns the first blocking lock if any
        This is used by the F_GETLK command
    """
    def test_lock_and_get_first_blocking(self, filepath, directory_id, filename, lock):
        dt = time.time()
        dt_timeout = dt + Mongo.configuration.lock_timeout()

        gf = Mongo.cache.find_one(self.files_coll, {'directory_id': directory_id, 'filename': filename})
        if gf is None:
            raise FuseOSError(errno.ENOENT)
        else:
            if 'lock' not in gf or ('lock' in gf and len(gf['lock'])) == 0:
                return None
            else:
                locks = list(filter(lambda l: l['creation'] < dt_timeout, gf['lock']))
                if len(locks) == 0:
                    return None
                else:
                    only_own_lock = len(locks) == 1 and locks[0]['id'] == lock['id']
                    own_lock_present = len(list(filter(lambda l: l['id'] == lock['id'], locks))) > 0
                    if only_own_lock or (lock['type'] == GenericFile.LOCK_UNLOCK and own_lock_present) or (lock['type'] == GenericFile.LOCK_SHARED and locks[0].type == GenericFile.LOCK_SHARED):
                        return None
                    else:
                        return locks[0]

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
        dir = Mongo.cache.find_one(self.files_coll, {'directory_id':previous_directory_id,'filename':first_directory_name,'generic_file_type': GenericFile.DIRECTORY_TYPE})
        if dir is not None:
            return self.get_last_directory_id_for_filepath(filepath='/'.join(elems[1:]), previous_directory_id=dir['_id'])

        return None

    """
        Increment/reduce the number of links for a directory 
    """
    def add_nlink_directory(self, directory_id, value):
        # You cannot update directly the object from gridfs, you need to do a MongoDB query instead
        Mongo.cache.find_one_and_update(self.files_coll, {'_id':directory_id}, {'$inc':{'metadata.st_nlink':value}})


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
        data = b''
        for chunk in Mongo.cache.find(self.chunks_coll, {'files_id':file._id,'n':{'$gte':starting_chunk,'$lte':ending_chunk}}):
            ending_size = min(starting_offset + size, chunk_size)
            data += chunk['data'][starting_offset:ending_size]

            size -= (ending_size - starting_offset)
            starting_offset = 0
        return data

    """
        Add data to a file. 
         file: Instance of a "File" type object.
         data: bytes 
         use_cache: True by default, can only be set to "False" if called by add_data_to_write
    """
    def add_data(self, file, data, offset, use_cache=True):
        # Normally, we should not update a gridfs document, but re-write everything. I don't see any specific reason
        # to do that, so we will try to update it anyway. But we will only rewrite the last chunks of it, or add information
        # to them, while keeping the limitation of ~255KB/chunk

        # We try to cache data
        if use_cache is True:
            return self.add_data_to_write(file=file, data=data, offset=offset)

        # Final size after the update
        total_size = offset + len(data)

        # Important note: the data that we receive are replacing any existing data from "offset".
        chunk_size = file.chunkSize
        total_chunks = int(ceil(file.length / chunk_size))
        starting_chunk = int(floor(offset / chunk_size))
        starting_byte = offset - starting_chunk * chunk_size
        if starting_byte < 0:
            print('Computation error for offset: '+str(offset))
        for chunk in Mongo.cache.find(self.chunks_coll, {'files_id':file._id,'n':{'$gte':starting_chunk}}):
            chunk['data'] = chunk['data'][0:starting_byte] + data[0:chunk_size-starting_byte]
            Mongo.cache.find_one_and_update(self.chunks_coll, {'_id':chunk['_id']},{'$set':{'data':chunk['data']}})

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
            chunks = []
            for i in range(0, remaining_chunks):
                chunk = {
                    "files_id": file._id,
                    "data": data[0:chunk_size],
                    "n": total_chunks
                }
                chunks.append(chunk)

                # We have written a part of what we wanted, we only the keep the remaining
                data = data[chunk_size:]

                # Next entry
                total_chunks += 1
            Mongo.cache.insert_many(self.chunks_coll, chunks)

        # We update the total length and its date and that's it
        dt = time.time()
        Mongo.cache.find_one_and_update(self.files_coll, {'_id':file._id},{
            '$set':{
                'length':total_size,
                'metadata.st_size':total_size,
                'metadata.st_blocks':GenericFile.size_to_blocks(total_size),
                'metadata.st_mtime': dt,
                'metadata.st_atime': dt,
                'metadata.st_ctime': dt
            }
        })

        return True

    """
        Keep a cache to write large chunks of data more efficiently. If the data becomes too big (> 10MB), flush it
        directly to the file. Otherwise it waits for the appropriate "flush" called at the end of the operation.
    """
    def add_data_to_write(self, file, data, offset):
        # The cache used here is not smart at all: We only cache successive data in a file, we do not care about specific
        # part of the file modified. As soon as the order is not strictly respected, we flush the cache to MongoDB.
        key = str(file.directory_id) + '/' + file.filename
        if key not in self.data_cache:
            self.data_cache[key] = {'offset':offset,'data':bytearray(b'')}

        # Check if we need to flush the cache
        max_size = 10*1024*1024
        if self.data_cache[key]['offset'] + len(self.data_cache[key]['data']) != offset or len(self.data_cache[key]['data']) >= max_size:
            print('Writting to another part of the file, flush the previous data.')
            self.add_data(file=file, data=bytes(self.data_cache[key]['data']), offset=self.data_cache[key]['offset'], use_cache=False)
            # Reset the cache for the new entry we will just add
            self.data_cache[key] = {'offset':offset,'data':bytearray(b'')}

        new_data = bytearray(data)
        self.data_cache[key]['data'] += new_data

        return True

    """
        Flush the cache for a specific file
    """
    def flush_data_to_write(self, file):
        key = str(file.directory_id) + '/' + file.filename
        if key in self.data_cache:
            self.add_data(file=file, data=bytes(self.data_cache[key]['data']), offset=self.data_cache[key]['offset'], use_cache=False)
            del self.data_cache[key]
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
        Mongo.cache.delete_many(self.chunks_coll, {'files_id':file._id,'n':{'$gte':maximum_chunks}})

        # We update the last chunk
        if length % chunk_size != 0:
            last_chunk = Mongo.cache.find_one(self.chunks_coll, {'files_id':file._id,'n':maximum_chunks-1})
            last_chunk['data'] = last_chunk['data'][0:length % chunk_size]
            Mongo.cache.find_one_and_update(self.chunks_coll, {'_id':last_chunk['_id']},{'$set':{'data':last_chunk['data']}})

        # We update the total length and that's it
        dt = time.time()
        Mongo.cache.find_one_and_update(self.files_coll, {'_id':file._id},{
            '$set': {
                'length': length,
                'metadata.st_size': length,
                'metadata.st_blocks': GenericFile.size_to_blocks(length),
                'metadata.st_mtime': dt,
                'metadata.st_atime': dt,
                'metadata.st_ctime': dt
            }
        })
        return True

    """
        Rename a generic file to another name
         generic_file: Instance of a GenericFile type object
         destination_filename: Expected filename
    """
    def rename_generic_file_to(self, generic_file, initial_filepath, destination_filepath):
        # There is no need to verify if the destination directory exists, and if there is not already a file with the same name,
        # as FUSE will automatically verify those conditions before calling our implementation of file moving.

        destination_directory = GenericFile.get_directory(filepath=destination_filepath)
        if not GenericFile.has_user_access_right(destination_directory, GenericFile.WRITE_RIGHTS):
            print('No rights to write on folder ' + destination_directory.filename)
            raise FuseOSError(errno.EACCES)

        # First we decrease the number of nlink in the directory above (even if we might stay in the same repository
        # at the end, that's not a big deal)
        initial_directory_id = GenericFile.get_directory_id(filepath=initial_filepath)
        self.add_nlink_directory(directory_id=initial_directory_id, value=-1)

        # We rename it
        destination_directory_id = destination_directory._id
        dest_filename = destination_filepath.split('/')[-1]
        Mongo.cache.find_one_and_update(self.files_coll, {'_id':generic_file._id},{'$set':{'directory_id':destination_directory_id,'filename':dest_filename}})

        # We increase the number of nlink in the final directory
        self.add_nlink_directory(directory_id=destination_directory_id, value=-1)

    """
        Remove locks for a generic file 
    """
    def release_generic_file(self, filepath, generic_file):
        lock_id = self.lock_id(filepath)
        Mongo.cache.find_one_and_update(self.files_coll,{'_id': generic_file._id}, {'$pull': {'lock': {'id': lock_id}}})
        return True

    """
        Update some arbitrary fields in the general "files" object
    """
    def basic_save(self, generic_file, metadata, attrs, host, uname, gname):
        Mongo.cache.find_one_and_update(self.files_coll, {'_id': generic_file._id}, {'$set': { 'metadata':metadata, 'attrs':attrs, 'host': host, 'gname': gname, 'uname': uname}})

    """
        Clean the database, only for development purposes
    """
    def clean_database(self):
        Mongo.cache.drop(self.chunks_coll)
        Mongo.cache.drop(self.files_coll)
