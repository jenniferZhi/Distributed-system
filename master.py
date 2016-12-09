import collections
import heapq
from multiprocessing import Process
import os,shutil,socket,threading,time,json
import worker
import sh
import signal
import time
import queue

class Master:

    # main thread of Master
    def __init__(self,num_workers, port_number):
        self.num_workers = num_workers
        self.port_number = port_number


        # worker status list
        self.worker_status = []
        self.ready_worker_list = []
        self.busy_worker_list = []

        self.workers_stamp = []

        self.process = []


        # current job information
        self.current_job_index = None
        self.current_output_dir = []
        self.current_mapper_executable = []
        self.current_reducer_executable = []

        # job queue
        self.job_queue = queue.Queue()
        self.job_processing = False

        # Step 1 : Create new folder
        tmp_dir = "var"
        tmp_dir_exists = os.path.isdir(tmp_dir)
        if (tmp_dir_exists):
            shutil.rmtree(tmp_dir)
        os.mkdir(tmp_dir)

        # Step 2 : Create a new TCP socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", port_number))
        s.listen(20)

        # Step 3 : Create a new thread
        t = threading.Thread(target=self.do_setup_thread,args=(num_workers,port_number))
        t.start()

        # Step 4 : Main thread start listen
        while True:
            clientsocket, address = s.accept()
            max_data = 1024
            all_data = " "

            while True:
                message = clientsocket.recv(max_data)
                all_data += message.decode("utf-8")

                if len(message) != max_data:
                    # Message done. Do something. End loop.
                    break
            json_data = json.loads(all_data)
            self.handle_msg(json_data, port_number)

            clientsocket.close()

    def worker(self,worker_number,worker_port_number,master_port,master_heartbeat_port):
        
        worker_ = worker.Worker(worker_number,worker_port_number,master_port,master_heartbeat_port)


    def do_setup_thread(self,num_workers,port_number):
        #create given number of worker process
        #process = []
        master_port = port_number
        master_heartbeat_port = master_port -1

        # Step 6 in startup : Create two threads
        heartbeat_thread = threading.Thread(target=self.listen_heartbeat, args=(master_heartbeat_port,))
        heartbeat_thread.start()
        fault_tolerance_thread = threading.Thread(target=self.fault_check, args=(master_heartbeat_port,))
        fault_tolerance_thread.start()


        # Step 5 in startup : Create worker process
        # worker_number :   The index (unique ID) of the worker
        # port_number :     The primary TCP socket port that this Worker should listen on
        # master_port :     The TCP socket that the Master is actively listening on (same as the port_number in the Master Constructor)    
        # master_heartbeat_port : the port that Master is listening for heartbeat messages
        #for i in range(1):
        for i in range(num_workers):
            worker_port_number = master_port + i + 1
            p = Process(target=self.worker, args=(i, worker_port_number, master_port, master_heartbeat_port))
            self.process.append(p)
            p.start()
            # Update worker status
            worker_info = {
                "worker_id" : i,
                "worker_pid" : p.pid,
                "worker_port_number" : worker_port_number,
                "worker_status" : "created"
            }
            if i == len(self.worker_status):
                self.worker_status.append(i)
                self.worker_status[i] = []
                self.worker_status[i].append(worker_info)
                self.worker_status[i].append([])

            else:
                self.worker_status[i] = []
                self.worker_status[i].append(worker_info)
                self.worker_status[i].append([])

        p.join()

        
        return 


    def listen_heartbeat(self,master_heartbeat_port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print("listen")

        s.bind(("127.0.0.1", master_heartbeat_port))

        while True:
            message, address = s.recvfrom(1024)
            tempmessage = message.decode()
            data = json.loads(tempmessage)
            #print(data)
            #print(data['message_type'])    

            # update workers_stamp info
            worker_id = data['worker_number']
            timestamp = time.time()
            worker_stamp = {
                "worker_id": worker_id,
                "timestamp": timestamp
            }

            # justify whether the workers_stamp dict has the current worker info
            check = 0
            for item in self.workers_stamp:
                if item['worker_id'] == worker_id:
                    check = 1
                    item['timestamp'] = timestamp

            if check == 0:
                self.workers_stamp.append(worker_stamp) 
            
            #print("workers stamp info:")
            #print(self.workers_stamp)       

    def fault_check(self,master_heartbeat_port):


        while True:
            time_limit = time.time() - 10

            for item in self.workers_stamp:
                if item['timestamp'] < time_limit:
                    print("something die")
                    # update worker info in worker_status dict
                    worker_id = item['worker_id']
                    self.worker_status[worker_id][0]['worker_status'] = "dead"

                    # kill the original process

                    try:
                        os.kill(self.worker_status[worker_id][0]['worker_pid'], signal.SIGKILL)
                    except OSError:
                        print("some error happens")
                    #kill = sh.Command("kill")
                    #kill(self.worker_status[worker_id][0]['worker_pid'])

                    # delete relative worker info in ready worker list

                    index = 0
                    for item in self.ready_worker_list:
                        if worker_id ==  item['worker_id']:
                            self.ready_worker_list.pop(index)
                            break
                        index = index + 1


                    print(self.ready_worker_list)

                    # restart the proceess
                    master_port = self.port_number
                    worker_port_number = master_port + worker_id + 1                    
                    master_heartbeat_port = master_port - 1


                    print("success success success")
                    p = Process(target=self.worker, args=(worker_id, worker_port_number, master_port, master_heartbeat_port))
                    p.start()
                    self.worker_status[worker_id][0]['worker_pid'] = p.pid
                    self.process.append(p)
                    #p.join()
                    time.sleep(10)
                    
                    

        print("fault_check")

    def handle_msg(self,message, port_number):
        if message['message_type'] == 'status':

            # If the status in respond message is "ready", add this worker_id to the ready_worker_list
            if message['status'] == 'ready':

                # update ready_worker_list
                worker_id = message['worker_number']

                worker_info = {
                    "worker_id" : worker_id
                }

                self.ready_worker_list.append(worker_info)

                print(self.ready_worker_list)

                if self.job_queue.empty() == False and self.job_processing == False:
                    op = self.job_processing.get()
                    self.current_job_index = op[0]
                    message = op[1]
                    self.job_processing = True
                    self.do_mapping_job(message, self.port_number)

                    return


                # try tp resend message to the relive worker
                if self.worker_status[worker_id][0]['worker_status'] == "dead":
                    
                    if self.worker_status[worker_id][1] != []:

                        send_message = self.worker_status[worker_id][1]                        
                        print("resend message:")
                        print(send_message)

                        # update worker status to the relative status
                        if send_message['executable'] == self.current_reducer_executable:
                            self.worker_status[worker_id][0]['worker_status'] = "reducing"
                        else:
                            self.worker_status[worker_id][0]['worker_status'] = "mapping"

                        # resend job to the worker

                        host = "127.0.0.1"
                        port = self.port_number + worker_id + 1

                        temp = json.dumps(send_message)

                        try:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect((host, port))
                            sock.sendall(str.encode(temp))
                            sock.close()
                            print("successfully send message")
                        except Exception as error:
                            print("Failed to send job to master.")

                    else:

                        self.worker_status[worker_id][0]['worker_status'] = "finished"

                
                else:
                    self.worker_status[worker_id][0]['worker_status'] = message['status']



                return
            


            elif message['status'] == 'finished':
                
                worker_id = message['worker_number']

                if self.worker_status[worker_id][0]['worker_status'] == 'mapping':

                    self.worker_status[worker_id][0]['worker_status'] = message['status']
                    self.worker_status[worker_id][1] = []

                    count_finish_worker = 0
                    for item in self.ready_worker_list:
                        i = item['worker_id']
                        if self.worker_status[i][0]['worker_status'] == 'finished':
                            count_finish_worker = count_finish_worker + 1
                        else:
                            break                
                    
                    if count_finish_worker == len(self.ready_worker_list):

                        # Notification:
                        # Required to remember job counter in the global variable
                        # do grouping job

                        print("\n")
                        print("mapping job finished")
                        print("\n")

                        counter = self.current_job_index;
                        
                        input_dir = "./var/job-{}/mapper-output".format(counter)
                        output_dir = "./var/job-{}/grouper-output".format(counter)
                        
                        grouper_filenames = self.__staff_run_group_stage(input_dir, output_dir, len(self.ready_worker_list))

                        output_reduce_dir = "var/job-{}/reducer-output".format(counter)

                        print("grouper name is as followed:")
                        print(grouper_filenames)
                        print(type(grouper_filenames))

                        self.do_reducing_job(port_number, grouper_filenames)


                        return
                elif self.worker_status[worker_id][0]['worker_status'] == 'reducing':

                    self.worker_status[worker_id][0]['worker_status'] = message['status']
                    self.worker_status[worker_id][1] = []

                    count_finish_worker = 0
                    for item in self.ready_worker_list:
                        i = item['worker_id']
                        if self.worker_status[i][0]['worker_status'] == 'finished':
                            count_finish_worker = count_finish_worker + 1
                        else:
                            break 

                    if count_finish_worker == len(self.ready_worker_list):


                        print("\n")
                        print("reducing job finished")
                        print("\n")

                        # do move job in finish period
                        output_dir = self.current_output_dir
                        output_dir_exists = os.path.isdir(output_dir)
                        if (output_dir_exists):
                            shutil.rmtree(output_dir)
                        #os.mkdir(output_dir)

                        counter = self.current_job_index
                        oldfile_dir = "var/job-{}/reducer-output".format(counter)
                        shutil.copytree(oldfile_dir, output_dir)
                        

                        print("\n")
                        print("All jobs have been finished")
                        print("\n")

                        self.job_processing = False
                        self.current_job_index = None

                        if self.job_queue.empty() == False and self.job_processing == False:
                            op = self.job_queue.get()
                            self.current_job_index = op[0]
                            message = op[1]
                            self.job_processing = True
                            self.do_mapping_job(message, self.port_number)

                        return
                        
                
        elif message['message_type'] == 'new_master_job':


            counter = 0;
            path = "var/job-{}".format(counter)
            while(os.path.isdir(path)):
                counter = counter + 1
                path = "var/job-{}".format(counter)

            os.mkdir(path)
            os.mkdir(os.path.join(path, "mapper-output"))
            os.mkdir(os.path.join(path, "grouper-output"))
            os.mkdir(os.path.join(path, "reducer-output"))

            #os.mkdir(path+"/mapper-output")
            #os.mkdir(path+"/grouper-output")
            #os.mkdir(path+"/reducer-output")
            #self.job_queue.put((counter, message))


            if len(self.ready_worker_list) != 0 and self.job_processing == False:
                if (self.job_queue.qsize() == 0):
                    self.current_job_index = counter
                    self.job_processing = True
                    self.do_mapping_job(message, self.port_number)
                else:
                    self.job_queue.put((counter, message))
                    
                    op = self.job_queue.get()
                    self.current_job_index = op[0]
                    message = op[1]
                    self.job_processing = True
                    self.do_mapping_job(message, self.port_number)
            else:
                self.job_queue.put((counter, message))

            return
        elif message['message_type'] == 'shutdown':

            print("running processes : "+str(self.process))
            for item in self.process:
                item.terminate()
                time.sleep(0.1)


            print(self.process)
            
            
            return

            

    def do_reducing_job(self, port_number, grouper_filenames):


        print("\n")
        print("reducing job begin")
        print("\n")
        

        master_port = port_number
        
        counter = self.current_job_index
        tmp_dir3 = "var/job-{}/reducer-output".format(counter)
        
        i = 0
        while(i < len(grouper_filenames)):
            worker_port_number = master_port + self.ready_worker_list[i]['worker_id'] + 1
            port = worker_port_number
            host = "127.0.0.1"

            self.worker_status[self.ready_worker_list[i]['worker_id']][0]['worker_status'] = "reducing"



            input_orange = []
            input_orange.append(grouper_filenames[i])


            print(type(input_orange))

            send_message = {
                "message_type" : "new_worker_job",
                "input_files" : input_orange,
                "executable" : self.current_reducer_executable,
                "output_directory" : tmp_dir3
            }


            # Add current message to worker_status
            self.worker_status[self.ready_worker_list[i]['worker_id']][1] = send_message



            temp = json.dumps(send_message)
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                sock.sendall(str.encode(temp))
                sock.close()
            except OSError:
                print("Failed to send job to master.")

            i = i + 1

        return


    def do_mapping_job(self, message, port_number):

        print("\n")
        print("mapping job begin")
        print("\n")

        print(self.num_workers)
            

        # Scan the input directory and divide the input files in 'X' equal parts
        # suppose we have four workers that could execute jobs
            
        counter = self.current_job_index

        ready_worker_num = len(self.ready_worker_list)

        input_dir = message['input_directory']

        # update current job information
        self.current_output_dir = message['output_directory']
        self.current_mapper_executable = message['mapper_executable']
        self.current_reducer_executable = message['reducer_executable']   

        # divide the input files in 'X' equal parts (where 'X' is the number of ready workers)
        i = 0
        filename = []
        input_files = []
        for in_filename in os.listdir(input_dir):
            filename = os.path.join(input_dir, in_filename)
            if len(input_files) == int(i % ready_worker_num): 
                input_files.append(int(i % ready_worker_num))
                input_files[int(i % ready_worker_num)] = []
                input_files[int(i % ready_worker_num)].append(filename)
            else:
                input_files[int(i % ready_worker_num)].append(filename)
            i = i + 1

        i = 0
        master_port = port_number
            
        host = "127.0.0.1"

        # send job to each ready worker
        while(i < len(input_files)):
            worker_port_number = master_port + self.ready_worker_list[i]['worker_id'] + 1
            port = worker_port_number

            send_message = {
                "message_type" : "new_worker_job",
                "input_files" : input_files[i],
                "executable" : self.current_mapper_executable,
                "output_directory" : "var/job-{}/mapper-output".format(counter)
            }
            #print(send_message)
            self.worker_status[self.ready_worker_list[i]['worker_id']][0]['worker_status'] = "mapping"
            

            # Add current message to worker_status
            #if len(self.worker_status[self.ready_worker_list[i]['worker_id']]) == 1 :
            #    self.worker_status[self.ready_worker_list[i]['worker_id']].append(send_message)
            #else:
            self.worker_status[self.ready_worker_list[i]['worker_id']][1] = send_message


            #print(self.worker_status)
            temp = json.dumps(send_message)
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                sock.sendall(str.encode(temp))
                sock.close()
            except OSError:
                print("Failed to send job to master.")
                    
            i = i + 1


  
    def __staff_run_group_stage(self, input_dir, output_dir, num_workers):
        # Loop through input directory and get all the files generated in Map stage
        filenames = []

        for in_filename in os.listdir(input_dir):
            #filename = input_dir + in_filename
            filename = os.path.join(input_dir, in_filename)
            # Open file, sort it now to ease the merging load later
            with open(filename, 'r') as f_in:
                content = sorted(f_in)

            # Write it back into the same file
            with open(filename, 'w+') as f_out:
                f_out.writelines(content)

            # Remember it in our list
            filenames.append(filename)

        # Create a new file to store ALL the sorted tuples in one single
        sorted_output_filename = os.path.join(output_dir, 'sorted.out')
        sorted_output_file = open(sorted_output_filename, 'w+')

        # Open all files in a single map command! Python is cool like that!
        files = map(open, filenames)

        # Loop through all merged files and write to our single file above
        for line in heapq.merge(*files):
            sorted_output_file.write(line)

        sorted_output_file.close()

        # Create a circular buffer to distribute file among number of workers
        grouper_filenames = []
        grouper_fhs = collections.deque(maxlen=num_workers)

        for i in range(num_workers):
            # Create temp file names
            basename = "file{0:0>4}.out".format(i)
            filename = os.path.join(output_dir, basename)

            # Open files for each worker so we can write to them in the next loop
            grouper_filenames.append(filename)
            fh = open(filename, 'w')
            grouper_fhs.append(fh)

        # Write lines to grouper output files, allocated by key
        prev_key = None
        sorted_output_file = open(os.path.join(output_dir, 'sorted.out'), 'r')

        for line in sorted_output_file:
            # Parse the line (must be two strings separated by a tab)
            tokens = line.rstrip().split("\t", 2)
            assert len(tokens) == 2, "Error: improperly formatted line"
            key, value = tokens

            # If it's a new key, then rotate circular queue of grouper files
            if prev_key != None and key != prev_key:
                grouper_fhs.rotate(1)

            # Write to grouper file
            fh = grouper_fhs[0]
            fh.write(line)

            # Update most recently seen key
            prev_key = key

        # Close grouper output file handles
        for fh in grouper_fhs:
            fh.close()

        # Delete the sorted output file
        sorted_output_file.close()
        os.remove(sorted_output_filename)

        # Return array of file names generated by grouper stage
        return grouper_filenames
