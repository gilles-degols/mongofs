#!/usr/bin/env python
from pymongo import MongoClient
import gridfs
from core.Configuration import Configuration
from core.GenericFile import GenericFile

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

        self.gridfs_files_collection = Mongo.configuration.mongo_prefix() + 'files.files'

    """
        Establish a connection to mongodb
    """
    def connect(self):
        mongo_path = 'mongodb://' + ','.join(Mongo.configuration.mongo_hosts())
        Mongo.instance = MongoClient(mongo_path)

    """
        List absolute filepathes in a given directory. 
    """
    def list_filenames(self, directory):
        filenames = []
        print('List files in "'+directory+'"')
        for elem in self.gridfs.find({'directory':directory}, no_cursor_timeout=True):
            print('Found filename...')
            filenames.append(elem.filename)
        return filenames

    """
        Create a generic file in gridfs. 
    """
    def create_generic_file(self, file):
        f = self.gridfs.new_file(**file.json)
        f.close()

    """
        Indicates if the generic file exists or not. 
    """
    def generic_file_exists(self, filename):
        return self.get_generic_file(filename=filename) is not None

    """
        Retrieve any file / directory / link document from Mongo. Returns None if none are found.
    """
    def get_generic_file(self, filename):
        return self.gridfs.find_one({'filename': filename})

    """
        Increment/reduce the number of links for a directory 
    """
    def add_nlink_directory(self, directory, value):
        # You cannot update directly the object from gridfs, you need to do a MongoDB query instead
        coll = self.database[self.gridfs_files_collection]
        coll.find_one_and_update({'filename':directory},
                                {'$inc':{'metadata.st_nlink':value}})

    """
        Clean the database, only for development purposes
    """
    def clean_database(self):
        self.database[self.gridfs_collection+'.chunks'].drop()
        self.database[self.gridfs_collection+'.files'].drop()
