import sys, os
from client import Client
from common import load_config

config = load_config("config/client.json")
client_instance = Client(config)

with open("config/help.txt", 'r') as f:
    help_text = f.read()

command = sys.argv[1]
if sys.argv[2] == "/": sys.argv[2] = ""
if command == "ls":
    path = sys.argv[2]
    path_str = path if path != "" else "/"
    print("current path: {} \n--------\n{}".format(path_str, client_instance.list_path(path)))
elif command == "mkdir":
    father_dir = sys.argv[2]
    dir_name = sys.argv[3]
    client_instance.mkdir(father_dir, dir_name)
elif command == "create":
    path = sys.argv[2]
    name = sys.argv[3]
    fid = client_instance.create(path, name)
    print("Create success, file id: {}, remote file path: {}/{}".format(
        fid, path, name))
elif command == "upload":
    local_path = sys.argv[2]
    remote_path = sys.argv[3]
    remote_name = None
    if len(sys.argv) == 5:
        remote_name = sys.argv[4]
    fid = client_instance.upload(local_path, remote_path, remote_name)
    if remote_name is None:
        remote_name = os.path.split(local_path)[-1]
    print("Upload success, file id: {}, remote file path: {}/{}".format(
        fid, remote_path, remote_name))
elif command == "read":
    fid_or_path = sys.argv[2]
    offset = int(sys.argv[3])
    length = int(sys.argv[4])
    contents = b""
    if fid_or_path.isnumeric():
        contents = client_instance.read(fid=fid_or_path, path=None, offset=offset, length=length)
    else:
        contents = client_instance.read(fid=None, path=fid_or_path, offset=offset, length=length)
    if len(sys.argv) == 6:
        out_path = sys.argv[5]
        with open(out_path, 'wb') as f:
            f.write(contents)
    else:
        print(contents)
elif command == "readall":
    fid_or_path = sys.argv[2]
    contents = b""
    if fid_or_path.isnumeric():
        contents = client_instance.read_all(fid=fid_or_path, path=None)
    else:
        contents = client_instance.read_all(fid=None, path=fid_or_path)
    if len(sys.argv) == 4:
        out_path = sys.argv[3]
        with open(out_path, 'wb') as f:
            f.write(contents)
    else:
        print(contents)
elif command == "write":
    fid_or_path = sys.argv[2]
    offset = int(sys.argv[3])
    from_file = sys.argv[4].lower()
    from_file = True if from_file == "true" else False
    if from_file:
        local_path = sys.argv[5]
        with open(local_path, 'rb') as f:
            contents = f.read()
    else:
        contents = sys.argv[5]
    if fid_or_path.isnumeric():
        client_instance.write(fid=fid_or_path, path=None, offset=offset, contents=contents)
    else:
        client_instance.write(fid=None, path=fid_or_path, offset=offset, contents=contents)
elif command == "file_status":
    fid_or_path = sys.argv[2]
    if fid_or_path.isnumeric():
        status = client_instance.get_file_statue_from_namenode(fid=fid_or_path, path=None)
    else:
        status = client_instance.get_file_statue_from_namenode(fid=None, path=fid_or_path)
    print("File status for {}\n-----\n".format(fid_or_path))
    print("file id: {}\n".format(status["fid"]))
    print("size: {}\n".format(status["size"]))
    print("block list: \n")
    for block in status["block_list"]:
        bid = block["bid"]
        print("\tblock id: {}, data_length: {}, datanodes: {}\n".format(bid, block["length"], status["datanodes"][bid]))
    print("\n")
else:
    print(help_text)
