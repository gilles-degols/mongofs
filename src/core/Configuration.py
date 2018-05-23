import json

"""
    Every configuration variable must be accessed through a method.
"""
class Configuration:
    # This value can be overrided by the user
    FILEPATH = '/etc/mongofs/mongofs.json'

    def __init__(self):
        self.load(filepath=Configuration.FILEPATH)

    """
        Read application
    """
    def load(self, filepath):
        with open(filepath, 'r') as f:
            self.conf = json.load(f)

    """
        Returns a list of host:port
    """
    def mongo_hosts(self):
        return self.conf['mongo']['hosts']

    """
        Return the database for the current filesystem instance
    """
    def mongo_database(self):
        return self.conf['mongo']['database']

    """
        Return the collection prefix for the current filesystem instance.
        Already contains the optional separator (like "_")
    """
    def mongo_prefix(self):
        return self.conf['mongo']['prefix']
