# TCP server example
import socket
import argparse
import traceback
import configuration
import tuner


class streamJit:
	def __init__(self, port):
		self.port = port
		self.program = "streamApp"
		print self.port

	def listen(self):
		try:
			server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_socket.bind(("localhost", self.port))
			server_socket.listen(1)
			client_socket, address = server_socket.accept()
			print "I got a connection from ", address
			self.socket = client_socket
			self.file = client_socket.makefile("rb")	# map the socket to file for higher performance and convienence.
			server_socket.close()
		except Exception, e:
			print "Exception occured : %s"%e
			traceback.print_exc()
			data = raw_input ( "Press Keyboard to exit..." )
			
	def run(self):
		print "Going to listen from socket and react"
		while 1:
		        data = self.file.readline()
		        if ( data == 'exit\n'):
				print data, "I have received exit. I am gonna exit."
				self.socket.close()
				break;
			elif ( data == 'program\n'):
				self.program = self.file.readline()
				
			elif ( data == 'confg\n' ):
				print "Config received."
				cfgString = self.file.readline()
				try:
					cfg = configuration.getConfiguration(cfgString)
					argv = ['--program', self.program]
					tuner.start(argv, cfg, self.socket, self)
				except Exception, e:
					print "Exception occured : %s"%e
					traceback.print_exc()
					data = raw_input ( "Press Keyboard to exit..." )
			else:
				print "###Invalid data received. Please check...:" , data

	def sendmsg(self, msg):
		if not msg.endswith("\n"):
			msg = msg + "\n"
		self.socket.send(msg)

	def recvmsg(self):
		data = self.file.readline()
		return data		

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('port', help='TCP port number', type=int)
	args = parser.parse_args()
	s = streamJit(args.port)
	s.listen()
	s.run()
		
