import pickle
import os

class IdManager(object):
    def __init__(self):
        self.current = 0
        self.id_pool = []
        self.id_set = set()

    def id_in_use(self, id_value):
        return id_value < self.current and id_value not in self.id_set

    def return_id(self, id_value):
        assert id_value not in self.id_set and id_value < self.current
        self.id_pool.append(id_value)
        self.id_set.add(id_value)
    
    def request_id(self):
        new_id = None
        if len(self.id_pool) > 0:
            new_id = self.id_pool.pop()
            self.id_set.discard(new_id)
        else:
            new_id = self.current
            self.current += 1
        return new_id
    
    def save(self, path):
        path = os.path.normpath(os.path.expanduser(path))
        dir_path = os.path.dirname(path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        arguments = {
            "current": self.current,
            "id_pool": self.id_pool
        }
        with open(path, 'wb') as f:
            pickle.dump(arguments, f)
    
    def load(self, path):
        path = os.path.normpath(os.path.expanduser(path))
        with open(path, 'rb') as f:
            arguments = pickle.load(f)
        self.current = arguments["current"]
        self.id_pool = arguments["id_pool"]
        self.id_set = set(self.id_pool)
