import json
from bson import json_util
"""
    Some useful tools for the various tests
"""
class Utils:
    def __init__(self, mongo=None):
        if mongo is None:
            Configuration.FILEPATH = 'test/resources/conf/mongofs.json'
            mongo = Mongo()

        self.mongo = mongo
        self.file = None
        self.file_raw = None
        self.directory = None
        self.directory_raw = None
        self.symbolic_link_file = None
        self.symbolic_link_file_raw = None
        self.directory_file = None
        self.directory_file_raw = None

    def load_files(self):
        # Load various files as setUp
        with open('test/resources/data/file.json', 'r') as f:
            self.file_raw = json_util.loads(f.read())
        self.file = self.mongo.load_generic_file(self.file_raw)

        with open('test/resources/data/file-chunks.json', 'r') as f:
            self.file_chunks_raw = json_util.loads(f.read())

        with open('test/resources/data/directory.json', 'r') as f:
            self.directory_raw = json_util.loads(f.read())
        self.directory = self.mongo.load_generic_file(self.directory_raw)

        with open('test/resources/data/symbolic-link.json', 'r') as f:
            self.symbolic_link_raw = json_util.loads(f.read())
        self.symbolic_link_file = self.mongo.load_generic_file(self.symbolic_link_raw)

        with open('test/resources/data/directory-file.json', 'r') as f:
            self.directory_file_raw = json_util.loads(f.read())
        self.directory_file = self.mongo.load_generic_file(self.directory_file_raw)

    def insert_file(self):
        self.mongo.files_coll.insert_one(self.file_raw)

    def insert_file_chunks(self):
        self.mongo.chunks_coll.insert_many(self.file_chunks_raw)

    def insert_directory(self):
        self.mongo.files_coll.insert_one(self.directory_raw)

    def insert_directory_file(self):
        self.mongo.files_coll.insert_one(self.directory_file_raw)

    def insert_symbolic_link(self):
        self.mongo.files_coll.insert_one(self.symbolic_link_raw)

    def read_file_chunks(self):
        message = b''
        for chunk in self.mongo.chunks_coll.find({'files_id':self.file._id}):
            message += chunk['data']
        return message