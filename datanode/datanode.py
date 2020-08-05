import os
import hashlib
from common import file_default_config, Block


class DataNode(object):
    def __init__(self, config):
        self.config = config
        self.block_table = {}

        # default file settings
        for k in file_default_config.keys():
            if k not in self.config:
                self.config[k] = file_default_config[k]

        # local path prefix
        self.config["prefix"] = os.path.normpath(
            os.path.expanduser(self.config["prefix"]))
        if not os.path.exists(self.config["prefix"]):
            os.makedirs(self.config["prefix"])

    def create_and_recv_block(self, blockinfo, block):
        """ receive a whole block """
        # check block length
        if len(block) > self.config["block_size"]:
            return {
                "success": False, 
                "message": "block too big. (expect <={}, got {})".format(self.config["block_size"], len(block))
            }        
        local_prefix = self.config["prefix"]
        path = os.path.join(local_prefix, "{}.blk".format(blockinfo["bid"]))
        with open(path, 'wb+') as f:
            f.write(block)
        return {
            "success": True,
            "message": self.block_md5(block)
        }

    def recv_block(self, blockinfo: dict, block):
        local_prefix = self.config["prefix"]
        path = os.path.join(local_prefix, "{}.blk".format(blockinfo["bid"]))
        offset = blockinfo.get("offset", 0)
        length = len(block)
        if offset + length > self.config["block_size"]:
            return {
                "success": False, 
                "message": "block out of range (expect <={}, got {})".format(
                    self.config["block_size"], length + offset)
            }
        with open(path, 'ab') as f:
            f.truncate(offset)
            f.write(block)
        
        with open(path, 'rb') as f:
            checksum = self.block_md5(f.read())
        return {
            "success": True,
            "message": checksum
        }

    def read_block(self, blockinfo):
        bid = blockinfo["bid"]
        length = blockinfo["length"]
        offset = blockinfo.get("offset", 0)
        path = os.path.join(self.config["prefix"], "{}.blk".format(bid))
        if not os.path.exists(path):
            return {"success": False, "message": "Block not exist."}, None
        with open(path, 'rb') as f:
            f.seek(offset)
            content = f.read(length)
        return {"success": True, "message": None}, content

    def block_md5(self, blk):
        return hashlib.md5(blk).hexdigest()
