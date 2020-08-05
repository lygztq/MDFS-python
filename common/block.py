class Block(object):
    def __init__(self, size:int, content_size=0, needId=False, bid=None):
        if needId:
            self.bid = id(self)
        else:
            assert isinstance(bid, int)
            self.bid = bid
        self.size = size
        self.content_size = content_size
    
    def full(self):
        return self.size == self.content_size
    
    def empty(self):
        return self.content_size == 0

    def blockinfo(self):
        return {"bid": self.bid, "size": self.size, "length": self.content_size}
