"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import tempfile
import click
import mapreduce.utils
import socket
import hashlib
from threading import Thread
import subprocess


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.manager_port = manager_port
        self.manager_host = manager_host
        self.port = port
        self.host = host

        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        connection = Thread(target=self.tcp_connect, args=())
        connection.start()
        # # TODO: you should remove this. This is just so the program doesn't
        # # exit immediately!
        # LOGGER.debug("IMPLEMENT ME!")
        # time.sleep(120)

    def map_job(self, job_dict):
        executable = job_dict["executable"]
        input_path = job_dict["input_paths"]
        task_id = job_dict["task_id"]
        num_partitions = int(job_dict["num_partitions"])

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)

            # Creating a file for each partition
            for partition in range(0, num_partitions):
                file_name = f"maptask{task_id:05d}-part{partition:05d}"
                LOGGER.info("Created file at %s", tmpdir  + file_name)
                open(tmpdir + file_name, "x")

            with open(input_path) as infile:
                with subprocess.Popen(
                    [executable],
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    text=True,
                ) as map_process:
                    for line in map_process.stdout:
                        # hashing to find the partition of each key value pair
                        hexdigest = hashlib.md5(line.vencode("utf-8")).hexdigest()
                        keyhash = int(hexdigest, base=16)
                        partition_number = keyhash % num_partitions
                        # Add line to correct partition output file
                        file_name = f"maptask{task_id:05d}-part{partition:05d}"
                        LOGGER.info("Hashed filename to %s", tmpdir  + file_name)
                        file1 = open("file_name", "a")  # append mode
                        file1.write(line+'\n')

        done_message = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": str(self.host),
            "worker_port": int(self.port)
        }
        self.message_to_manager(self.host, self.port, done_message)


    def tcp_connect(self):
        """Connect to TCP socket for recieving messages."""
        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            # sends registration message
            reg_message = {
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            self.message_to_manager(self.host, self.port, reg_message)

            sock.settimeout(1)

            while True:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                clientsocket.settimeout(1)

                # Receive data, one chunk at a time.  
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                if message_dict["message_type"] == "register_ack":
                    self.heartbeat_thread()
                elif message_dict["message_type"] == "shutdown":
                    print(message_dict)
                    break
                elif message_dict["message_type"] == "new_map_task":
                    self.map_job(message_dict)
                print(message_dict)

    def message_to_manager(self, host, port, message_dict):
        """Send registration message to manager."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect((self.manager_host, self.manager_port))
            
            message = json.dumps(message_dict)
            LOGGER.info("Worker sending message: %s", message) 
            sock.sendall(message.encode('utf-8'))

    def send_heartbeat(self):
        """Send heartbeat to manager"""
        # create an INET, DGRAMing socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # connect to the server
            sock.connect((self.manager_host, self.manager_port))

            # send a heartbeat to manager
            heartbeat_msg = {
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            message = json.dumps(heartbeat_msg)
            sock.sendall(message.encode('utf-8'))

    def heartbeat_thread(self):
        """Once we receive register_ack, send heartbeat."""
        heartbeat = Thread(target=self.send_heartbeat, args=())
        heartbeat.start()
        

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
