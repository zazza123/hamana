class HamanaException(Exception):
    """
        General exception of Hamana library
    """
    def __init__(self, description):
        self.description = description

    def __str__(self):
        return self.description

class HamanaDatabaseAlreadyInitialised(HamanaException):
    """
        Exception to raise when HamanaDatabase is already initialised
    """
    def __init__(self):
        super().__init__("HamanaDatabase is already initialised.")

class HamanaDatabaseNotInitialised(HamanaException):
    """
        Exception to raise when HamanaDatabase is not initialised.
    """
    def __init__(self):
        super().__init__("No instance of HamanaDatabase is initialised.")