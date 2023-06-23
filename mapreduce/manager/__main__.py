"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
from threading import Thread
from time import sleep
import timeit
import socket
import shutil


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker_Meta:
    """Represent Worker Metadata."""
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.state = "ready"

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        # member variable for stopping heart
        self.shutdown = False
        self.host = host
        self.port = port

        # state goes between ready and busy
        self.state = "ready"
        # queue for worker tracking
        self.worker_queue = []
        self.job_queue = []

        self.job_id = 0
        self.tasks_to_complete = 0

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        check_workers = Thread(target=self.heart_messages, args=())
        check_workers.start()
        
        job_runner = Thread(target=self.job_runner, args=())
        job_runner.start()

        self.tcp_connect()
        self.shutdown = True
        # why is this needed?
        check_workers.join()
        job_runner.join()

    def ready_worker(self):
        """Finds the index of the most early ready worker."""
        ready_worker_index = -1
        LOGGER.info("Searching for worker...num workers total: %s", len(self.worker_queue) )
        while ready_worker_index == -1:
            for worker in range(0, len(self.worker_queue)):
                if self.worker_queue[worker].state == "ready":
                    ready_worker_index = worker
                    self.worker_queue[worker].state = "busy"
                    break
            if ready_worker_index == -1: # haven't found a worker - avoid busy waiting
                time.sleep(0.1)
        return ready_worker_index
        
    def job_runner(self):
        """Manages running of jobs."""
        while(self.shutdown == False):
            # whenever the old job is finished and theres something in the job queue, assign a job
            if(self.tasks_to_complete == 0 and len(self.job_queue) > 0):
                
                # Make output directory
                curr_job = self.job_queue.pop()
                output_dir = curr_job["output_directory"]

                if os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                os.makedirs(output_dir)

               # make temp directory for processing
                curr_job_id = curr_job["job_id"]
                prefix = f"mapreduce-shared-job{curr_job_id:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)

                    # round robin mappers
                    # grab input files
                    curr_job_input_dir = curr_job["input_directory"]
                    input_files = os.listdir(curr_job_input_dir)

                    # mapper partitions - array of arrays
                    mapper_partitions = []
                    for i in range(0, curr_job["num_mappers"]):
                        mapper_partitions.append([])
                    curr_index = 0
                    for file in input_files:
                        mapper_partitions[curr_index % curr_job["num_mappers"]].append(file)
                        curr_index += 1
                    self.tasks_to_complete = len(mapper_partitions)
                    LOGGER.info("Partitioned job %s", mapper_partitions)

                    # send jobs to workers - reconstruct dictionary
                    # handling sending job dicts for dictionary
                    partition_ind = 0
                    while len(mapper_partitions) != 0:

                        curr_partition = mapper_partitions.pop()
                        
                        for i in range(0, len(curr_partition)):
                            head_tail = os.path.split(curr_partition[i])
                            LOGGER.info("Changing this input path %s", curr_partition[i])
                            curr_partition[i] = head_tail[1] # last part of path
                        LOGGER.info("Changed partition to raw files %s", curr_partition)

                        ready_worker_index = self.ready_worker()
                        LOGGER.info("Found a ready worker %s", ready_worker_index)

                        job_desc = {
                            "message_type": "new_map_task",
                            "task_id": partition_ind,
                            "input_paths": curr_partition,
                            "executable": curr_job["mapper_executable"],
                            "output_directory": curr_job["output_directory"],
                            "num_partitions": curr_job["num_reducers"],
                            "worker_host": str(self.worker_queue[ready_worker_index].host),
                            "worker_port": int(self.worker_queue[ready_worker_index].port)
                        }
                        partition_ind += 1
                        self.message_to_worker(self.worker_queue[ready_worker_index].host,
                            self.worker_queue[ready_worker_index].port, job_desc)
                        
                    while (self.shutdown == False) :
                        time.sleep(0.1)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            else:
                # wait (avoid busy-waiting though)
                time.sleep(0.1)


    def heart_messages(self):
        """Recieve message."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)

            # No sock.listen() since UDP doesn't establish connections like TCP

            # Receive incoming UDP messages
            while self.shutdown == False:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                print(message_dict)

    def message_to_worker(self, worker_host, worker_port, message_dict):
        """Send message to worker using a new socket."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect((worker_host, worker_port))

            # send a message to the worker
            message = json.dumps(message_dict)

            LOGGER.info("Manager sending message: %s", message) 

            sock.sendall(message.encode('utf-8'))
    
    
    def tcp_connect(self):
        """Set up listening for worker messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()

            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)

            while True:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)

                # Receive data, one chunk at a time.  If recv() times out before we
                # can read a chunk, then go back to the top of the loop and try
                # again.  When the client closes the connection, recv() returns
                # empty data, which breaks out of the loop.  We make a simplifying
                # assumption that the client will always cleanly close the
                # connection.
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
                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock1:
                    if (message_dict["message_type"] == "register"):
                        new_message_dict = {
                            "message_type": "register_ack",
                            "worker_host": str(message_dict["worker_host"]),
                            "worker_port": int(message_dict["worker_port"]),
                        }
                        # ready worker tracking
                        self.worker_queue.append(Worker_Meta(str(message_dict["worker_host"]), int(message_dict["worker_port"])))
                        self.message_to_worker(str(message_dict["worker_host"]), int(message_dict["worker_port"]), new_message_dict)
                    elif (message_dict["message_type"] == "shutdown"):
                        # Shutdown all threads!
                        new_message_dict = {
                            "message_type" : "shutdown",
                        }
                        # Sends shutdown message to all workers
                        for worker in self.worker_queue:
                            self.message_to_worker(str(worker.host), int(worker.port), new_message_dict)
                        # Shuts itself down
                        break
                    elif (message_dict["message_type"] == "new_manager_job"):
                        new_job_dict = message_dict
                        new_job_dict["job_id"] = self.job_id
                        self.job_id += 1
                        self.job_queue.append(new_job_dict)
                    elif (message_dict["message_type"] == "finished"):
                        self.tasks_to_complete  -= 1
                        for worker in self.worker_queue:
                            if(message_dict["worker_host"] == worker.host and
                                message_dict["worker_port"] ==  worker.port):
                                worker.state = "ready"
                    
                        
                print(message_dict)


# TODO: Create function for fault tolerence thread (When a worker dies, how we deal with that)

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
