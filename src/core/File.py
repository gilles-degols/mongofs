from core.GenericFile import GenericFile

class File(GenericFile):
    """
        Add data to the existing file (it might already have some)
    """
    def add_data(self, data, offset):
        GenericFile.mongo.add_data(file=self, data=data, offset=offset)

    """
        Truncate a file to a specific size
    """
    def truncate(self, length):
        GenericFile.mongo.truncate(file=self, length=length)