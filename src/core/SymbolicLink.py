from core.GenericFile import GenericFile

class SymbolicLink(GenericFile):
    """
        Return the target of the symbolic link
    """
    def get_target(self):
        print(self.json)
        return self.json['target']
