from typing import Union
import logging

class INode(object):
    def __init__(self, name, root=False, parent=None):
        self.name = name
        self.parent = parent
        if root:
            self.parent = None
            self.name = ""

    def is_root(self) -> bool:
        return self.name == ""

    def abspath(self) -> str:
        if self.is_root():
            return ""
        else:
            return "{}/{}".format(self.parent.abspath(), self.name)
    
    def detach(self):
        self.parent.remove_child(self.name)
        self.parent = None
        return self

    def remove_child(self, name):
        pass
    
    def list_self(self) -> str:
        return self.name + "\n"

    def is_file(self) -> bool:
        pass

    def is_dir(self) -> bool:
        pass

    def insert_child(self, node):
        pass

    def delete(self):
        pass

    def find_child(self, name):
        return None
    
    def collect_fids(self):
        pass

    def find_by_path(self, path:str):
        current_level_pos = path.find('/')
        if current_level_pos == -1:
            return self
        else:
            next_path = path[current_level_pos+1:]
            next_pos = next_path.find('/')
            if next_pos == -1:
                return self.find_child(next_path)
            child_name = next_path[:next_pos]
            child = self.find_child(child_name)
            if child is None: 
                logging.warning("No such file or dictionary: {}".format(child_name))
                return child
            return child.find_by_path(next_path)

    def deep_copy(self):
        pass

    def empty(self) -> bool:
        pass


class INodeFile(INode):
    def __init__(self, name, file_id=None, root=False, parent=None):
        super().__init__(name, root=root, parent=parent)
        self.file_id = file_id
    
    def empty(self):
        return self.file_id is None
    
    def is_file(self):
        return True

    def is_dir(self):
        return False

    def collect_fids(self) -> int:
        return self.file_id
    
    def delete(self):
        self.file_id = None
        self.detach()

    def deep_copy(self) -> INode:
        new_node = INodeFile(self.name, self.file_id, root=self.is_root())
        return new_node


class INodeDirectory(INode):
    def __init__(self, name, root=False, parent=None):
        super().__init__(name, root=root, parent=parent)
        self.children = {}

    def empty(self):
        return len(self.children) == 0

    def remove_child(self, name):
        if name in self.children:
            self.children.pop(name)
    
    def list_self(self):
        if self.is_root():
            return_str = ".\n"
        else:
            return_str = ".\n..\n"
        for child_name, child in self.children.items():
            if child.is_file():
                return_str += "{} -> fid: {}\t{}\n".format(child_name, child.file_id, "type: file")
            else:
                return_str += "{}\t\t{}\n".format(child_name, "type: directory")
        return return_str

    def is_file(self):
        return False

    def collect_fids(self) -> list:
        out = []
        for child_name in self.children:
            fids = self.children[child_name].collect_fids()
            if isinstance(fids, int):
                out.append(fids)
            else:
                out.extend(fids)
        return out

    def is_dir(self):
        return True
    
    def insert_child(self, node:INode) -> bool:
        if self.find_child(node.name) is None:
            node.parent = self
            self.children[node.name] = node
            return True
        else:
            logging.error("{}/{} has already exist".format(self.abspath(), node.name))
            return False
    
    def delete(self):
        if self.is_root():
            return
        names = self.children.keys()
        for name in names:
            self.children[name].delete()
        self.detach()
    
    def find_child(self, name) -> Union[INode, None]:
        return self.children.get(name)

    def deep_copy(self) -> INode:
        new_node = INodeDirectory(self.name, self.is_root())
        for name, child in self.children.items():
            new_node[name] = child.deep_copy()
        return new_node


class FileSystemTree(object):
    def __init__(self):
        self.root = INodeDirectory("", root=True)

    def mkdir(self, path, name) -> Union[None, INode]:
        """ make a new directory given the name, if fail, return None """
        parent_node = self.root.find_by_path(path)
        if parent_node is None or not parent_node.is_dir():
            error_str = "Fail to make new directory {} in {}: path does not exist or is not a directory".format(name, path)
            logging.error(error_str)
            return error_str

        new_dir = INodeDirectory(name, root=False, parent=parent_node)
        parent_node.insert_child(new_dir)
        return new_dir
    
    def create(self, dir_path, fid, file_name) -> Union[None, INode]:
        parent_node = self.root.find_by_path(dir_path)
        if parent_node is None or not parent_node.is_dir():
            logging.error(
                "Fail to create new file {} in {}: path does not exist or is not a directory".format(file_name, dir_path))
            return None
        new_file_node = INodeFile(file_name, file_id=fid, parent=parent_node)
        parent_node.insert_child(new_file_node)
        return new_file_node

    def collect_fids(self, path) -> list:
        node = self.find_by_path(path)
        if node is not None:
            return node.collect_fids()
        else:
            return []

    def delete(self, path):
        node = self.root.find_by_path(path)
        if node is None: 
            logging.warning("{}: No such file or directory".format(path))
            return
        node.delete()

    def find_by_path(self, path) -> Union[None, INode]:
        return self.root.find_by_path(path)

    def list_node(self, path) -> str:
        node = self.find_by_path(path)
        if node is None:
            logging.error("{}: No such file or directory".format(path))
            return ""
        return node.list_self()
    
    def exist(self, path) -> bool:
        return self.root.find_by_path(path) is not None
