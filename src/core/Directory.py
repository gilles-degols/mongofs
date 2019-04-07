#!/usr/lib/mongofs/environment/bin/python
from src.core.GenericFile import GenericFile

class Directory(GenericFile):
    """
        Indicates if the current GenericFile is in fact a directory
    """
    def is_dir(self):
        return True
