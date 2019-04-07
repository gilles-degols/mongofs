#!/usr/lib/mongofs/environment/bin/python
import os
import subprocess
import time
from expiringdict import ExpiringDict

from pymongo.errors import NetworkTimeout, AutoReconnect, ConnectionFailure
from pymongo import MongoClient
import gridfs
from src.core.Configuration import Configuration
from pymongo.collection import ReturnDocument

from functools import wraps

"""
    Custom decorator to easily handle a MongoDB disconnection.
    We can (should) even use the wrapper in the connect / load method 
"""
def retry_connection(view_func):
    # It can be difficult to kill the mount because of fusepy. Ideally we would need the latest version of fusepy but they
    # didn't make a release for quite some times.
    def custom_kill(config):

        command = '/usr/bin/fusermount -u ' + str(config.mounting_point) + ''
        print('Try to umount the current file system: ' + str(command))
        # Run in background
        # TODO: It seems it never works to umount correctly with python directly. But if we enter the same command ourselves, or during
        # the startup, it works...
        subprocess.run([command], shell=True, stdout=subprocess.PIPE)

        # We wait a bit, as we hope the umount will work.
        time.sleep(15)

        # In that case, the mount point will be "corrupted", and only fixed once we try to mount again.
        print('It seems the umount did not work, kill the process itself.')

        SIGKILL = 9
        os.kill(os.getpid(), SIGKILL)


    def _decorator(*args, **kwargs):
        new_connection_attempt = False
        st = time.time()
        mongo_cache = args[0]
        while True:
            try:
                if new_connection_attempt is True:
                    time.sleep(0.5)
                    mongo_cache.connect()
                    mongo_cache.load_internal()
                # To easily see the queries done
                # print('Run mongo query: '+str(args)+' with '+str(kwargs))
                response = view_func(*args, **kwargs)
                return response
            except (NetworkTimeout, AutoReconnect, ConnectionFailure) as e:
                dt = time.time() - st
                if dt >= mongo_cache.configuration.mongo_access_attempt():
                    print('Problem to execute the query, maybe we are disconnected from MongoDB. ' +
                          'Max access attempt exceeded ('+str(int(dt))+'s >= '+str(mongo_cache.configuration.mongo_access_attempt())+'). ' +
                          'Stop the mount.')
                    custom_kill(mongo_cache.configuration)
                    # We want to exit the current loop
                    exit(1)
                else:
                    print('Problem to execute the query, maybe we are disconnected from MongoDB. Connect and try again.')
                    new_connection_attempt = True

    return wraps(view_func)(_decorator)

"""
    This class will implement every method that we need to connect to MongoDB, and every query should be run through it (with the exceptions of tests). 
    This is also an easy to handle the disconnection to MongoDB during a short amount of time.
"""
class MongoCache:
    instance = None
    configuration = None
    cache = None

    def __init__(self):
        # We reuse the same connexion
        if MongoCache.instance is None:
            MongoCache.configuration = Configuration()
            self.reset_cache()
            retry_connection(self.connect())
        retry_connection(self.load_internal())

    """
        Reset the cache completely. This should be done every time we delete / update more than 1 thing to avoid problems.
    """
    def reset_cache(self):
        MongoCache.cache = ExpiringDict(max_len=MongoCache.configuration.cache_max_elements(), max_age_seconds=MongoCache.configuration.cache_timeout())
        MongoCache.data_cache = ExpiringDict(max_len=MongoCache.configuration.data_cache_max_elements(), max_age_seconds=MongoCache.configuration.data_cache_timeout())

    """
        Establish a connection to mongodb
    """
    def connect(self):
        mongo_path = 'mongodb://' + ','.join(MongoCache.configuration.mongo_hosts())
        MongoCache.instance = MongoClient(mongo_path, w=MongoCache.configuration.mongo_write_acknowledgement(), j=MongoCache.configuration.mongo_write_j())

        # If we were disconnected from MongoDB, it would be wise to reset the cache
        self.reset_cache()

    """
        Create an index
    """
    @retry_connection
    def create_index(self, coll, index):
        return self.database[coll].create_index(index)


    """
        Get the objects to connect to the correct database and collections
    """
    def load_internal(self):
        self.instance = MongoCache.instance
        self.database = MongoCache.instance[MongoCache.configuration.mongo_database()]

        # We use gridfs only to store the files. Even if we have a lot of small files, the overhead should
        # still be small.
        # Documentation: https://api.mongodb.com/python/current/api/gridfs/index.html
        gridfs_collection = MongoCache.configuration.mongo_prefix() + 'files'
        self.gridfs = gridfs.GridFS(self.database, gridfs_collection)

    """
        Simply retrieve any document
    """
    @retry_connection
    def find_one(self, coll, query):
        # Super important note: By allowing ">= 2" and not "=2" parameters, we open the door to potential problems as we do
        # not check the other fields in the query... As we only have a few of them, we can do the check ourselves, but
        # that's not pretty at all.
        if len(query) >= 2 and 'directory_id' in query and 'filename' in query:
            # In that case we check in the cache
            key = str(query['directory_id'])+'/'+query['filename']

            if key in MongoCache.cache:
                try:
                    # We do some manual checks
                    doc = MongoCache.cache[key]
                    valid_doc = True
                    if 'generic_file_type' in query:
                        valid_doc = valid_doc and doc['generic_file_type'] == query['generic_file_type']
                    if 'lock' in query and '$exists' in query['lock']:
                        if 'lock' in doc['lock'] and query['lock']['$exists'] is False:
                            valid_doc = False
                        elif 'lock' not in doc['lock'] and query['lock']['$exists'] is True:
                            valid_doc = False

                    if valid_doc is False:
                        return None
                    return doc
                except Exception as e:
                    # The document might be deleted as the clean up could occur just 1ms afterwards when we access some attributes
                    print('Exception while using the cache, it might happen some times (normally there should not be any impact): '+str(e))

            # Key not found in cache, we try to load the object and store it before returning it (only if the document exists)
            res = self.database[coll].find_one(query)
            if res is not None:
                MongoCache.cache[key] = res
            return res

        return self.database[coll].find_one(query)

    """
        A generic find function, which might be problematic to handle if we get a connection error while iterating on it.
        It needs to be handle on the caller side to avoid any problem.
    """
    @retry_connection
    def find(self, coll, query, projection=None):
        # We need a small data cache for some blocks
        if len(query) == 2 and 'files_id' in query and 'n' in query and '$gte' in query['n'] and '$lte' in query['n']:
            key = str(query['files_id'])+'/'+str(query['n']['$gte'])+'/'+str(query['n']['$lte'])
            if key in MongoCache.data_cache:
                # Be careful: the clean up could occur just 1ms afterwards when we access some attributes
                return MongoCache.data_cache[key]

            # Data not found in cache, we need to store it
            raw = list(self.database[coll].find(query, projection, no_cursor_timeout=True))
            MongoCache.data_cache[key] = raw
            return raw

        return self.database[coll].find(query, projection, no_cursor_timeout=True)

    """
        A FindOneAndUpdate which always return the document after modification
    """
    @retry_connection
    def find_one_and_update(self, coll, query, update):
        result = self.database[coll].find_one_and_update(query, update, return_document=ReturnDocument.AFTER)

        if coll.endswith('.files') and result is not None:
            # We directly update the document in the cache.
            key = str(result['directory_id']) + '/' + str(result['filename'])
            self.reset_cache()
            MongoCache.cache[key] = result
        elif coll.endswith('.chunks') and result is not None:
            # We simply reset the data cache to avoid complicating things if we update a document.
            self.reset_cache()

        return result

    """
        A simple insert_one
    """
    @retry_connection
    def insert_one(self, coll, document):
        return self.database[coll].insert_one(document)

    """
        A simple insert_many
    """
    @retry_connection
    def insert_many(self, coll, documents):
        return self.database[coll].insert_many(documents, ordered=False, bypass_document_validation=True)

    """ 
        A simple delete_many
    """
    @retry_connection
    def delete_many(self, coll, query):
        self.reset_cache()
        return self.database[coll].delete_many(query)

    """
        Create a new file with gridfs directly. Save it directly
    """
    @retry_connection
    def gridfs_new_file(self, file):
        f = self.gridfs.new_file(**file)
        f.close()

    """
        Delete a file in gridfs
    """
    @retry_connection
    def gridfs_delete(self, _id):
        self.reset_cache()
        return self.gridfs.delete(_id)

    """
        The drop command is only used for development normally
    """
    @retry_connection
    def drop(self, coll):
        self.reset_cache()
        return self.database[coll].drop()