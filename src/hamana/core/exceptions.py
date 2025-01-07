class HamanaException(Exception):
    """
        General exception of Hamana library
    """
    def __init__(self, description):
        self.description = description

    def __str__(self):
        return self.description