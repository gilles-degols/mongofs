#!/usr/bin/env python
import errno
from math import floor, ceil
from errno import ENOENT, EDEADLOCK
import time
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

from pymongo import MongoClient
import gridfs
from src.core.Configuration import Configuration
from pymongo.collection import ReturnDocument

"""
    This class will implement every method that we need to connect to MongoDB, and every query should be run through it (with the exceptions of tests). 
    This is also an easy to handle the disconnection to MongoDB during a short amount of time.
"""
class MongoCache:
    instance = None
    configuration = None

    def __init__(self):
        # We reuse the same connexion
        if MongoCache.instance is None:
            MongoCache.configuration = Configuration()
            self.connect()
        self.load_internal()

    """
        Establish a connection to mongodb
    """
    def connect(self):
        mongo_path = 'mongodb://' + ','.join(MongoCache.configuration.mongo_hosts())
        MongoCache.instance = MongoClient(mongo_path)


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
    def find_one(self, coll, query):
        return self.database[coll].find_one(query)

    """
        A generic find function, which might be problematic to handle if we get a connection error while iterating on it.
        It needs to be handle on the caller side to avoid any problem.
    """
    def find(self, coll, query, projection=None):
        return self.database[coll].find(query, projection, no_cursor_timeout=True)

    """
        A FindOneAndUpdate which always return the document after modification
    """
    def find_one_and_update(self, coll, query, update):
        return self.database[coll].find_one_and_update(query, update, return_document=ReturnDocument.AFTER)

    """
        A simple insert_one
    """
    def insert_one(self, coll, document):
        return self.database[coll].insert_one(document)

    """ 
        A simple delete_many
    """
    def delete_many(self, coll, query):
        return self.database[coll].delete_many(query)

    """
        Create a new file with gridfs directly. Save it directly
    """
    def gridfs_new_file(self, file):
        f = self.gridfs.new_file(**file)
        f.close()

    """
        Delete a file in gridfs
    """
    def gridfs_delete(self, _id):
        return self.gridfs.delete(_id)

    """
        The drop command is only used for development normally
    """
    def drop(self, coll):
        return self.database[coll].drop()