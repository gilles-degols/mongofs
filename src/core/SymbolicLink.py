#!/usr/lib/mongofs/environment/bin/python3.6
from src.core.GenericFile import GenericFile

class SymbolicLink(GenericFile):
    """
        Return the target of the symbolic link
    """
    def get_target(self):
        return self.json['target']

    """
        Indicates if the current GenericFile is in fact a link
    """
    def is_link(self):
        return True