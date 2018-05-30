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
            return 3600*24*365*100
        return self.conf['mongo']['access_attempt_s']

    """
        Return the maximum amount of time (in seconds) we can allow a lock to be set on a file without any operation on it. If the timeout
        happens, we release the lock (to avoid locking file eternity if there is a problem).
        Value <= 0 means infinity.
    """
    def lock_timeout(self):
        if self.conf['lock']['timeout_s'] <= 0:
            # Kinda infinite
            return 3600*24*365*100
        return self.conf['lock']['timeout_s']

    """ 
        Return the maximum amount of time (in seconds) we try to access a locked file before throwing an exception to the user.
        Value <= 0 means infinity.
    """
    def lock_access_attempt(self):
        if self.conf['lock']['access_attempt_s'] <= 0:
            # Kinda infinite
            return 3600*24*365*100
        return self.conf['lock']['access_attempt_s']

    """
        Return the hostname of the current server
    """
    def hostname(self):
        return self.conf['host']
