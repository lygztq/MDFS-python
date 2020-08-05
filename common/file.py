file_default_config = {
    "num_replicas": 3,
    "block_size": 2 * (2 ** 20)
}

class File(object):
    def __init__(self, name, block_list=None, need_id=False, fid=None):
        self.name = name
        self.block_list = [] if block_list is None else block_list
        self.fid = fid if not need_id else id(self)
        self.cum_size = None
        self.adjust()
    
    def adjust(self):
        self.cum_size = [0]
        tmp_size = 0
        for b in self.block_list:
            tmp_size += b.content_size
            self.cum_size.append(tmp_size)

    def add_block(self, block):
        self.block_list.append(block)
        self.adjust()

    def update_block(self, blockinfo):
        bid = int(blockinfo["bid"])
        for block in self.block_list:
            if bid == block.bid:
                block.content_size = blockinfo["length"]
                break
    
    def locate(self, offset):
        for i, v in self.cum_size:
            if offset < v:
                return i - 1
        return len(self.block_list) - 1

    def status(self):
        return {
            "fid": self.fid,
            "block_list": list(map(lambda b: b.blockinfo(), self.block_list)),
            "size": self.cum_size[-1]
        }
