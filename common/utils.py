import json
import logging
import os

def norm_path(path):
    return os.path.normpath(os.path.expanduser(path))

def load_config(path):
    path = norm_path(path)
    with open(path, 'r') as f:
        return json.load(f)

def locate(block_list, offset):
    cumsum = 0
    idx = 0
    for i, blockinfo in enumerate(block_list):
        cumsum += blockinfo["length"]
        if cumsum > offset:
            inner_offset = offset - cumsum + blockinfo["length"]
            bid = blockinfo["bid"]
            length = blockinfo["length"]
            idx = i
            break
    if offset >= cumsum:
        logging.error("offset out of range ({} >= {})".format(offset, cumsum))
        return None
    return i, {"bid": bid, "offset": inner_offset, "length": length}

def locate_interval(block_list, offset, length):
    if length <= 0:
        return None
    locate_result = locate(block_list, offset)
    if locate_result is None:
        return None
    start_idx, start_blockinfo = locate_result
    current_block = start_blockinfo
    block_info_list = []
    idx = start_idx
    while length > 0 and idx < len(block_list):
        if idx > start_idx:
            current_block = block_list[idx] 
        offset = current_block.get("offset", 0)
        new_block_info = {"bid": current_block["bid"], "offset": offset}
        if length + offset < current_block["length"]:
            new_block_info["length"] = length
            length = 0
        else:
            length -= (current_block["length"] - offset)
            new_block_info["length"] = current_block["length"] - offset
        block_info_list.append(new_block_info)
        idx += 1        
    if length > 0:
        logging.error("offset + length out of range")
        return None
    return block_info_list
