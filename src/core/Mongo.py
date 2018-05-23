#!/usr/bin/env python
from pymongo import MongoClient
import gridfs
from core.Configuration import Configuration
from core.GenericFile import GenericFile
from math import floor, ceil

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
        self.gridfs_chunks_collection = Mongo.configuration.mongo_prefix() + 'files.chunks'

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
        Add data to a file. 
         file: Instance of a "File" type object.
         data: bytes 
    """
    def add_data(self, file, data, offset):
        # Normally, we should not update a gridfs document, but re-write everything. I don't see any specific reason
        # to do that, so we will try to update it anyway. But we will only rewrite the last chunks of it, or add information
        # to them, while keeping the limitation of ~255KB/chunk
        coll_meta = self.database[self.gridfs_files_collection]
        coll = self.database[self.gridfs_chunks_collection]

        # Final size after the update
        total_size = offset + len(data)

        # Important note: the data that we receive are replacing any existing data from "offset".
        chunk_size = file.chunkSize
        total_chunks = int(ceil(file.length / chunk_size))
        starting_chunk = int(floor(offset / chunk_size))
        starting_byte = offset - starting_chunk * chunk_size
        if starting_byte < 0:
            print('Computation error for offset: '+str(offset))
        for chunk in coll.find({'files_id':file._id,'n':{'$gte':starting_chunk}}):
            chunk['data'] = chunk['data'][0:starting_byte] + data[0:chunk_size-starting_byte]
            coll.find_one_and_update({'_id':chunk['_id']},{'$set':{'data':chunk['data']}})

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
                total_chunks += 1
                chunk = {
                    "files_id": file._id,
                    "data": data[0:chunk_size],
                    "n": total_chunks
                }
                coll.save(chunk)

                # We have written a part of what we wanted, we only the keep the remaining
                data = data[chunk_size:]

        # We update the total length and that's it
        coll_meta.find_one_and_update({'_id':file._id},{'$set':{'length':total_size,'metadata.st_size':total_size}})

        return True

    """
        Truncate a part of a file 
         file: Instance of a "File" type object.
         length: Offset from which we need to truncate the file 
    """
    def truncate(self, file, length):
        coll_meta = self.database[self.gridfs_files_collection]
        coll = self.database[self.gridfs_chunks_collection]

        # We drop every unnecessary chunk
        chunk_size = file.chunkSize
        maximum_chunks = int(ceil(length / chunk_size))
        coll.delete_many({'files_id':file._id,'n':{'$gte':maximum_chunks}})

        # We update the last chunk
        if length % chunk_size != 0:
            last_chunk = coll.find_one({'files_id':file._id,'n':maximum_chunks-1})
            last_chunk = last_chunk['data'][0:length % chunk_size]
            coll.find_one_and_update({'_id':last_chunk['_id']},{'$set':{'data':last_chunk['data']}})

        # We update the total length and that's it
        coll_meta.find_one_and_update({'_id':file._id},{'$set':{'length':length,'metadata.st_size':length}})
        return True

    """
        Clean the database, only for development purposes
    """
    def clean_database(self):
        self.database[self.gridfs_collection+'.chunks'].drop()
        self.database[self.gridfs_collection+'.files'].drop()
