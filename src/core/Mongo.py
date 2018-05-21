#!/usr/bin/env python
from pymongo import MongoClient
import gridfs
from core.Configuration import Configuration

class Mongo:
    instance = None
    configuration = None

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

    """
        Establish a connection to mongodb
    """
    def connect(self):
        mongo_path = 'mongodb://' + ','.join(Mongo.configuration.mongo_hosts())
        Mongo.instance = MongoClient(mongo_path)

    """
        List files in a given directory. We only need to return a list of filenames (not the absolute path).
        A filename must not start with "/". 
    """
    def list_files(self, directory):
        filenames = []
        for elem in self.gridfs.find({'directory':directory}, no_cursor_timeout=True):
            # TODO PERF: Store a lookup filename -> object for the next operations (in less than 1s) asking
            # for the file size, ....
            filenames.append(elem.filename)
        return filenames
