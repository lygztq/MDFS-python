import socket
import json
import random
import logging
import os
import time
from common import file_default_config, Block, locate_interval, norm_path

socket.setdefaulttimeout(20)
class Client(object):
    def __init__(self, config):
        self.config = config
        for k in file_default_config.keys():
            if k not in self.config:
                self.config[k] = file_default_config[k]

    def divide_file(self, file_contains):
        block_size = self.config["block_size"]
        start = 0
        file_len = len(file_contains)
        blocks = []
        while start < file_len:
            end = min(start + block_size, file_len)
            blocks.append(file_contains[start:end])
            start += block_size
        return blocks

    def send_block_to_datanode(self, blockinfo, block_contents, datanode_addr:tuple):
        command = {
            "command": "write_block",
            "block_info": blockinfo
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect(datanode_addr)
            server_socket.send(bytes(json.dumps(command).encode('utf-8'))) # send command
            server_socket.recv(2048) # command ack
            # send block
            total_sent = 0
            length = len(block_contents)
            while total_sent < length:
                sent = server_socket.send(block_contents[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent += sent
            server_socket.recv(2048) # block ack
            server_socket.send(b"ack") # ready for result
            result = json.loads(server_socket.recv(2048).decode('utf-8'), encoding='utf-8') # get result head
            server_socket.send(b"ack") # result head ack
        if not result["success"]:
            print(result["message"]) # error message if not success
            return None
        else:
            return result["message"] # return checksum if success

    def send_full_block_to_datanode(self, blockinfo, block_contents, datanode_addr:tuple):
        command = {
            "command": "write_whole_block",
            "block_info": blockinfo
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect(datanode_addr)
            server_socket.send(bytes(json.dumps(command).encode('utf-8'))) # send command
            server_socket.recv(2048) # command ack
           # send block
            total_sent = 0
            length = len(block_contents)
            while total_sent < length:
                sent = server_socket.send(block_contents[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent += sent
            server_socket.recv(2048) # block ack
            server_socket.send(b"ack") # ready for result
            result = json.loads(server_socket.recv(2048).decode('utf-8'), encoding='utf-8') # get result head
            server_socket.send(b"ack") # result head ack
        if not result["success"]:
            logging.error(result["message"]) # error message if not success
            return None
        else:
            return result["message"] # return checksum if success

    def read_block_from_datanode(self, blockinfo, datanode_addr:tuple):
        command = {
            "command": "read_block",
            "block_info": blockinfo
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect(datanode_addr) # addr + port
            server_socket.send(bytes(json.dumps(command).encode('utf-8'))) # send command
            server_socket.recv(2048) # command ack
            server_socket.send(b"ack") # ready to recv result
            result = json.loads(server_socket.recv(2048).decode('utf-8')) # recv result head
            server_socket.send(b"ack") # result head ack
            
            # recv block
            chunks = []
            length = blockinfo["length"]
            success = True
            total_recd = 0
            while total_recd < length:
                chunk = server_socket.recv(min(2048, length - total_recd))
                if chunk == b"":
                    raise RuntimeError("socket connection broken")
                chunks.append(chunk)
                total_recd += len(chunk)
            block = b"".join(chunks)
        if result["success"]:
            return block
        else:
            logging.error(result["message"])
            return None

    def get_file_statue_from_namenode(self, fid=None, path=None):
        if fid is None and path is None:
            logging.fatal("at least one of fid and path must be given")
            exit(1)
        if fid is not None: fid = int(fid)
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "file_status",
            "args": { "fid": fid, "path": path }
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            result = server_socket.recv(16384).decode("utf-8")
            result:dict = json.loads(result)
            if not result["success"]:
                logging.fatal(result["message"])
                exit(1)
            else:
                result = result["message"]
            keys = result["datanodes"].keys()
            transformed_datanode_list = {}
            for k in keys:
                v = result["datanodes"][k]
                transformed_datanode_list[int(k)] = v
            result["datanodes"] = transformed_datanode_list
            return result
        
    # def delete(self, fid=None, path=None):
    #     pass
    
    def list_path(self, path):
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "list",
            "args": { "path": path }
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            result = server_socket.recv(2048).decode("utf-8")
            result = json.loads(result, encoding="utf-8")
            return result["message"]

    def request_new_block(self, fid):
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "new_block",
            "args": {"fid": fid}
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            result = server_socket.recv(2048).decode("utf-8")
            result = json.loads(result, encoding="utf-8")["message"]
            return result
    
    def update_file_status(self, fid, status):
        if fid is not None: fid = int(fid)
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "update_file_status",
            "args": {"fid": fid, "file_status": status}
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            server_socket.recv(2048)
        return True

    def update_block_info(self, fid, blockinfo):
        if fid is not None: fid = int(fid)
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "update_block_status",
            "args": {"fid": fid, "block_info": blockinfo}
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            server_socket.recv(2048)
        return True
    
    def read_all(self, fid=None, path=None):
        if fid is not None: fid = int(fid)
        status = self.get_file_statue_from_namenode(fid, path)
        file_contents = b""
        print("reading...")
        for block in status["block_list"]:
            bid = block["bid"]
            datanode = random.choice(status["datanodes"][bid])
            port = self.config["datanode_port"]
            datanode_addr = (datanode, port)
            err = None
            success = False
            block_data = None
            for t in range(5):
                try:
                    block_data = self.read_block_from_datanode(block, datanode_addr)
                    success = True
                except Exception as e:
                    err = e
                    time.sleep(2)
                    success = False
                if success:
                    break
            if not success:
                logging.fatal("something goes wrong when read files: {}".format(err))
                exit(1)
            if block_data is None:
                print("Error when read file {} block {}".format(fid, bid))
                return None
            file_contents += block_data
        return file_contents

    def read(self, fid, path, offset, length):
        if length <= 0: return
        if fid is not None: fid = int(fid)
        status = self.get_file_statue_from_namenode(fid, path)
        if offset + length > status["size"]:
            logging.fatal("max read index {} out of range (should be less than {})".format(offset + length, status["size"]))
            exit(1)
        file_contents = b""
        block_list_for_read = locate_interval(status["block_list"], offset, length)
        print("reading...")
        for block in block_list_for_read:
            datanode = random.choice(status["datanodes"][block["bid"]])
            port = self.config["datanode_port"]
            block_data = self.read_block_from_datanode(block, (datanode, port))
            if block_data is None:
                return None
            file_contents += block_data

        return file_contents

    def write(self, fid, path, offset, contents):
        if len(contents) == 0: return
        if fid is not None: fid = int(fid)
        if not isinstance(contents, bytes):
            contents = bytes(contents, encoding="utf-8")
        file_status = self.get_file_statue_from_namenode(fid, path)
        if offset > file_status["size"]:
            logging.fatal("offset {} out of range (shoule be less than {})".format(offset, file_status["size"]))
            exit(1)
        fid = file_status["fid"]
        diff = file_status["size"] % self.config["block_size"]
        writable_space = file_status["size"] if diff == 0 else file_status["size"] - diff + self.config["block_size"]
        if len(file_status["block_list"]) > 0:
            file_status["block_list"][-1]["length"] = self.config["block_size"]
        
        length = len(contents) # length of data to write
        writable_space = writable_space - offset # space of this file from offset to the end of last block
        length_inplace = min(length, writable_space) # space does not need allocation
        length_append = max(0, length - writable_space) # space need to allocate

        # first inplace space, we will discard info of blocks changed
        print("writing...")
        start = 0
        if len(file_status["block_list"]) > 0:
            block_list_for_write = locate_interval(file_status["block_list"], offset, length_inplace)
            last_bid = block_list_for_write[0]["bid"]
            idx = 0
            for i, b in enumerate(file_status["block_list"]):
                if b["bid"] == last_bid:
                    idx = i
                    break
            file_status["block_list"] = file_status["block_list"][:idx]
            
            for block in block_list_for_write:
                # write to datanode
                datanode = file_status["datanodes"][block["bid"]]
                port = self.config["datanode_port"]
                block_data = contents[start:start+block["length"]]
                last_checksum = None
                for node in datanode:
                    checksum = self.send_block_to_datanode(block, block_data, (node, port))
                    if last_checksum is not None and last_checksum != checksum:
                        logging.error("checksum error: {} != {}".format(checksum, last_checksum))
                        return None
                    last_checksum = checksum
                offset = block.get("offset", 0)
                file_status["block_list"].append({
                    "bid": block["bid"],
                    "length": offset + block["length"]
                })
                start += block["length"]
            
        # if have to append new blocks
        if length_append > 0:
            num_append_blocks = (length_append - 1) // self.config["block_size"] + 1
            last_length = length_append % self.config["block_size"]
            if last_length == 0: last_length = self.config["block_size"]
            for i in range(num_append_blocks):
                block_length = last_length if i == num_append_blocks-1 else self.config["block_size"]
                block_meta_dict = self.request_new_block(fid)
                block_info = block_meta_dict["block"]
                block_info["length"] = block_length
                datanodes = block_meta_dict["datanodes"]
                last_checksum = None
                block_data = contents[start:start+block_length]

                for node in datanodes:
                    checksum = self.send_full_block_to_datanode(
                        block_info, block_data, (node, self.config["datanode_port"]))
                    if checksum is None:
                        return False
                    if last_checksum is not None and last_checksum != checksum:
                        logging.error("checksum error: {} != {}".format(last_checksum, checksum))
                        return False
                    last_checksum = checksum
                file_status["block_list"].append(block_info)
                start += block_length
        self.update_file_status(fid, file_status)
        return True

    def upload(self, local_path, remote_path, remote_name=None):
        local_path = norm_path(local_path)
        file_name = os.path.split(local_path)[-1]
        if remote_name is None: remote_name = file_name
        fid = self.create(remote_path, remote_name)

        # load local file and divide into blocks
        with open(local_path, 'rb') as f:
            data = f.read()
        block_datas = self.divide_file(data)
        block_list = []

        print("uploading...")
        for block_data in block_datas:
            # request block in namenode
            block_meta_dict = self.request_new_block(fid)
            block_info = block_meta_dict["block"]
            datanodes = block_meta_dict["datanodes"]
            block_info["length"] = len(block_data)
            last_checksum = None

            # upload blocks to datanodes
            for node in datanodes:
                success = False
                err = None
                for t in range(5):
                    try:
                        checksum = self.send_full_block_to_datanode(
                            block_info, block_data, (node, self.config["datanode_port"]))
                        success = True
                    except Exception as e:
                        err = e
                        time.sleep(2)
                        success = False
                    if success: break
                if not success:
                    logging.fatal("something goes wrong when upload files: {}".format(err))
                    exit(1)
                if checksum is None:
                    return None
                if last_checksum is not None and last_checksum != checksum:
                    logging.error("checksum error: {} != {}".format(last_checksum, checksum))
                    return None
                last_checksum = checksum

            # update block info in namenode
            self.update_block_info(fid, block_info)

        return fid

    def create(self, path, file_name):
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "create",
            "args": {"path": path, "name": file_name}
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            result = json.loads(server_socket.recv(2048).decode('utf-8'))
            if not result["success"]:
                logging.fatal(result["message"])
                exit(1)
            else:
                fid = result["message"]
                return fid

    def mkdir(self, path, name):
        namenode_addr = self.config["namenode_addr"]
        namenode_port = self.config["namenode_port"]
        command = {
            "command": "mkdir",
            "args": {"path": path, "name": name}
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.connect((namenode_addr, namenode_port))
            server_socket.send(bytes(json.dumps(command).encode("utf-8")))
            result = json.loads(server_socket.recv(2048).decode('utf-8'))
            if not result["success"]:
                logging.fatal(result["message"])
                exit(1)
