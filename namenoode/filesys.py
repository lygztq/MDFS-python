import os, sys, time
import logging
from random import sample
from typing import Union

from common import File, Block, IdManager, file_default_config
from .filesys_tree import INode, FileSystemTree


class FileSystem(object):
    def __init__(self, config):
        self.file_table = {}
        self.datanode_table = []
        self.block_to_datanode = {}
        self.fid_to_path = {}
        self.config = config
        self.file_id_manager = IdManager()
        self.block_id_manager = IdManager()
        self.file_tree = FileSystemTree()
        
        for k in file_default_config.keys():
            if k not in self.config:
                self.config[k] = file_default_config[k]
        self.datanode_table = config["datanode_list"]
    
    def create(self, path="", name=None) -> dict:
        if not self.file_tree.exist(path):
            error_str = "{}: No such directory".format(path)
            logging.error(error_str)
            return {"success": False, "message": error_str}
        if self.file_tree.exist("{}/{}".format(path, name)):
            error_str = "{}/{}: File already exist".format(path, name)
            logging.error(error_str)
            return {"success": False, "message": error_str}
        new_fid = self.file_id_manager.request_id()
        newfile = File(name, fid=new_fid)
        self.file_table[newfile.fid] = newfile
        self.file_tree.create(path, new_fid, name)
        self.fid_to_path[new_fid] = "{}/{}".format(path, name)
        return {"success": True, "message": newfile.fid}
    
    def list_path(self, path) -> dict:
        res = self.file_tree.list_node(path)
        success = (res != "")
        if not success: res = "File or dir does not exist!"
        return {"success": success, "message": res}

    def delete(self, fid=None, path=None) -> dict:
        """Delete a file or directory

        Before send delete command to the namenode, The
        client must check this file or dir exists by send
        get_file_status command to get file_status. 

        Fid will be first considered when perform operations.
        If the file pointed by the path is not the same as the 
        file pointed by fid, the path argument will be ignored.
        """
        delete_dir = False
        if fid is None and path is None:
            error_str = "At least one of address and subscript must be given"
            logging.error(error_str)
            return {"success": False, "message": error_str}
        
        if fid is not None and fid in self.fid_to_path:
            path = self.fid_to_path[fid]
        elif path is not None:
            node = self.file_tree.find_by_path(path)
            if node is None:
                error_str = "{}: no such file".format(path)
                logging.error(error_str)
                return {"success": False, "message": error_str}
            delete_dir = node.is_dir()
            if not delete_dir:
                fid = node.file_id
        else:
            logging.error("invalid argument")
            return {"success": False, "message": "invalid argument"}
        
        if delete_dir:
            fids = self.file_tree.collect_fids(path)
            self.file_tree.delete(path)
            for fid in fids:
                self.file_table.pop(fid)
                self.fid_to_path.pop(fid)
        else:
            self.file_tree.delete(path)
            self.file_table.pop(fid)
            self.fid_to_path.pop(fid)

        return {"success": True, "message": None}

    def from_path_to_fid(self, path):
        node = self.file_tree.find_by_path(path)
        if node is None:
            return False, "file {} does not exist".format(path)
        if node.is_dir():
            return False, "{} is directory, not file path".format(path)
        return True, node.file_id

    def file_status(self, fid=None, path=None) -> dict:
        if path is not None:
            success, result = self.from_path_to_fid(path)
            if not success:
                return {"success": False, "message": result}
            fid = result
        if fid not in self.file_table:
            err = "{}".format(path if path is not None else fid)
            return {"success": False, "message": "file id {}: no such file!".format(err)}
        status = self.file_table[fid].status()
        datanodes = {}
        for b in status["block_list"]:
            datanodes[b["bid"]] = self.block_to_datanode[b["bid"]]
        status["datanodes"] = datanodes
        return {"success": True, "message":status}

    def request_new_block(self, fid) -> dict:
        fileobj = self.file_table[fid]
        new_bid = self.block_id_manager.request_id()
        newblock = Block(self.config["block_size"], bid=new_bid)
        bid = newblock.bid
        
        fileobj.block_list.append(newblock)
        fileobj.adjust()

        max_replicas = min(self.config["num_replicas"], len(self.datanode_table))
        datanodes = sample(self.datanode_table, max_replicas)
        self.block_to_datanode[bid] = datanodes
        return {"success": True, "message": {"block": newblock.blockinfo(), "datanodes": datanodes}}

    def update_file(self, fid, status):
        fileobj = self.file_table[fid]
        fileobj.block_list = status["block_list"]
        fileobj.block_list = list(map(
            lambda b: Block(self.config["block_size"], 
            b["length"], bid=b["bid"]), status["block_list"]))
        fileobj.adjust()

    def update_block(self, fid, block_info):
        fileobj:File = self.file_table[fid]
        fileobj.update_block(block_info)
        fileobj.adjust()
    
    def mkdir(self, path, name):
        res = self.file_tree.mkdir(path, name)
        if isinstance(res, str):
            return {"success": False, "message": res}
        else:
            return {"success": True, "message": None}

    def test_out(self):
        if not os.path.exists("log"):
            os.mkdir("log")
        with open("log/namenode_log", 'a') as f:
            f.write("Time: {}\n".format(time.strftime("%c")))
            for k, v in self.__dict__.items():
                f.write("{}: {}\n".format(k, v))
            f.write("\n")
