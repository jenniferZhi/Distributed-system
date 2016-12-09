import socket,threading,time,json
import sh
import os
class Worker:

	def __init__(self,worker_number,port_number,master_port,master_heartbeat_port):
		self.worker_number = worker_number
		self.port_number = port_number
		self.master_port = master_port
		self.master_heartbeat_port = master_heartbeat_port
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind(("127.0.0.1", port_number))
		print("orangesuccess")
		s.listen(20)

		print("Worker {} ".format(worker_number, ))

		# start a new thread
		t = threading.Thread(target=self.do_setup_thread,args=(worker_number,master_port,master_heartbeat_port))
		t.start()

		# keep listening message
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
			self.handle_msg(json_data)
			# should add the real working opearting process
			clientsocket.close()

	def do_setup_thread(self,worker_number,master_port,master_heartbeat_port):
		
		# Register for host and port
		host = "127.0.0.1"
		port = master_port

		# Send "Ready" Message to the Server
		message = {
			"message_type" : "status",
			"worker_number" : worker_number,
			"status" : "ready"
		}
		
		temp = json.dumps(message)
		'''
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((host, port))
		sock.sendall(str.encode(temp))
		print("successful send message")
		sock.close()
		'''
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((host, port))
			sock.sendall(str.encode(temp))
			print("successful send message")
			sock.close()
		except Exception as error:
			print("Failed to send message to master.")
		
		
		# Build up new heardbeat thread
		send_heartbeat_thread = threading.Thread(target=self.send_heartbeat, args=(worker_number,master_heartbeat_port))
		send_heartbeat_thread.start()

	def send_heartbeat(self,worker_number,master_heartbeat_port):
		s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		host = "127.0.0.1"
		port = master_heartbeat_port
		while True:
			message = {
			  "message_type": "heartbeat",
			  "worker_number": worker_number
			}
			temp = json.dumps(message)
			s.sendto(str.encode(temp),(host,port))
			time.sleep(2)

	def handle_msg(self, message):
		if message['message_type'] == 'new_worker_job':

			input_files = message['input_files']

			output_directory = message['output_directory']

			python = sh.Command("python")
			cat = sh.Command("cat")
			executable = message['executable']
			run = sh.Command(executable)


			#time.sleep(10)

			if type(input_files) == list:
				for path in input_files:

					filename = path.split('/')
					filename = filename[len(filename) - 1]
					#filename = os.path.join("./", output_directory, filename)
					filename = os.path.join(output_directory, filename)

					run(_in = open(path), _out = "{}".format(filename))
					#python(executable, _in = open(path), _out = "{}".format(filename))
			else:

				filename = input_files.split('/')
				filename = filename[len(filename) - 1]
				#filename = os.path.join("./",output_directory, filename)

				filename = os.path.join(output_directory, filename)

				run(_in = open(input_files), _out = "{}".format(filename))
				#python(executable, _in=open(input_files), _out="{}".format(filename))

			send_message = {
				"message_type" : "status",
				"worker_number" : self.worker_number,
				"status" : "finished"
			}
			temp = json.dumps(send_message)


			print("here is the type of worker_Number:")
			print(type(self.worker_number))
				
			host = "127.0.0.1"
			port = self.master_port

			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((host, port))
				sock.sendall(str.encode(temp))
				sock.close()
					
			except OSError:
				print("Failed to update status to master.")
			
			return

		return 1





