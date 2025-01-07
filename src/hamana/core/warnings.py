class HamanaWarning(Warning):
    """
        General warning of Hamana library
    """
    def __init__(self, description):
        self.description = description

    def __str__(self):
        return self.description