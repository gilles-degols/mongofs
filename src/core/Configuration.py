#!/usr/lib/mongofs/environment/bin/python
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

    """
        Maximum number of seconds we will try to reconnect to MongoDB if we lost the connection at inappropriate time.
        Value <= 0 means infinity.
    """
    def mongo_access_attempt(self):
        if self.conf['mongo']['access_attempt_s'] <= 0:
            # Kinda infinite
            return 3600 * 24 * 365 * 100
        return self.conf['mongo']['access_attempt_s']

    """
        Write concern to write to MongoDB. 0 disable write acknowledgement
        Value <= 0 means infinity.
    """
    def mongo_write_acknowledgement(self):
        return self.conf['mongo']['write_acknowledgement']

    """
        If set to True, wait for the MongoDB journaling to acknowledge the write
        Value <= 0 means infinity.
    """
    def mongo_write_j(self):
        return self.conf['mongo']['write_j']

    """
        Return the maximum amount of time (in seconds) we can allow a lock to be set on a file without any operation on it. If the timeout
        happens, we release the lock (to avoid locking file eternity if there is a problem).
        Value <= 0 means infinity.
    """
    def lock_timeout(self):
        if self.conf['lock']['timeout_s'] <= 0:
            # Kinda infinite
            return 3600 * 24 * 365 * 100
        return self.conf['lock']['timeout_s']

    """ 
        Return the maximum amount of time (in seconds) we try to access a locked file before throwing an exception to the user.
        Value <= 0 means infinity.
    """
    def lock_access_attempt(self):
        if self.conf['lock']['access_attempt_s'] <= 0:
            # Kinda infinite
            return 3600 * 24 * 365 * 100
        return self.conf['lock']['access_attempt_s']

    """ 
        Return the maximum amount of time (in seconds) we can keep the cache for a file. Can be very high if standalone
        MongoDB, should be reasonnable if we are in a cluster (a few seconds). Only used for the file attributes, not the
        data themselves.
        Value <= 0 means disabled.
    """
    def cache_timeout(self):
        if self.conf['cache']['timeout_s'] <= 0:
            return 0
        return self.conf['cache']['timeout_s']

    """
        Return the maximum number of entries we can keep in the cache.
    """
    def cache_max_elements(self):
        if self.conf['cache']['max_elements'] <= 0:
            return 0
        return self.conf['cache']['max_elements']

    """ 
        Return the maximum amount of time (in seconds) we can keep the data cache for a file. Only used for the data 
        themselves, so should not be too big.
        Value <= 0 means disabled.
    """
    def data_cache_timeout(self):
        if self.conf['data_cache']['timeout_s'] <= 0:
            return 0
        return self.conf['data_cache']['timeout_s']

    """
        Return the maximum number of entries we can keep in the data cache.
    """
    def data_cache_max_elements(self):
        if self.conf['data_cache']['max_elements'] <= 0:
            return 0
        return self.conf['data_cache']['max_elements']

    """
        Return the hostname of the current server
    """
    def hostname(self):
        return self.conf['host']

    """
        Indicates if we are in a development mode (= clean database before mount for example) or not.
    """
    def is_development(self):
        return self.conf.get('development', False)

    """
        The chunk size in gridfs. Value must be between 1 and 15MB maximum (to allow overhead of other fields)
    """
    def chunk_size(self):
        chunk_size = self.conf['mongo']['chunk_size']
        if chunk_size < 1 or chunk_size > 15 * 1024 * 1024:
            raise ValueError('Invalid chunk size, must be between 1 and ' + str(15 * 1024 * 1024) + ' bytes.')
        return chunk_size

    """
        Sets the default mode of the root node in mongofs
    """
    def default_root_mode(self):
        return int(self.conf.get('default_root_mode', '0755'), 8)

    """
        Sets always mode of the root node in mongofs
    """
    def force_root_mode(self):
        return self.conf.get('force_root_mode', False)
