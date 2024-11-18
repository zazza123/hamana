class HamanaException(Exception):
    """
        General exception of Hamana library
    """
    def __init__(self, description):
        self.description = description

    def __str__(self):
        return self.description

class HamanaDatabaseAlreadyInitialized(HamanaException):
    """
        Exception to raise when HamanaDatabase is already initialized
    """
    def __init__(self):
        super().__init__("HamanaDatabase is already initialized.")