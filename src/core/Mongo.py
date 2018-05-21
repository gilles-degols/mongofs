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

    """
        Establish a connection to mongodb
    """
    def connect(self):
        mongo_path = 'mongodb://' + ','.join(Mongo.configuration.mongo_hosts())
        Mongo.instance = MongoClient(mongo_path)

    """
        List files in a given directory
    """
    def list_files(self, directory):
        print('List files for '+directory)
        pass