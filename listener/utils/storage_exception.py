class StorageException:
    def __init__(self, error=None):
        self.error = None
    
    def set_error(self, error):
        self.error = error
    
    def get_error(self):
        if self.error == None:
            return "unknown error"
        return str(self.error)