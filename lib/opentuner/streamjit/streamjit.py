#!../../venv/bin/python
import socket
import argparse
import traceback
import configuration
import tuner
import sys


class streamJit:
	def __init__(self, port):
		self.port = port
		self.program = "streamApp"

	def listen(self):
		try:
			print 'listening for client at local prot %d'%self.port
			server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_socket.bind(("localhost", self.port))
			server_socket.listen(1)
			client_socket, address = server_socket.accept()
			print "Got a connection from ", address
			self.socket = client_socket
			self.file = client_socket.makefile("rb")	# map the socket to file for high performance and convienence.
			server_socket.close()
		except Exception, e:
			print "Exception occured : %s"%e
			traceback.print_exc()
			data = raw_input ( "Press Keyboard to exit..." )
			
	def run(self):
		while 1:
		        data = self.recvmsg()
		        if ( data == 'exit\n'):
				print data, "I have received exit. I am gonna exit."
				break;
			elif ( data == 'program\n'):
				self.program = self.file.readline()
				
			elif ( data == 'confg\n' ):
				print "Config received."
				cfgString = self.file.readline()
				try:
					cfg = configuration.getConfiguration(cfgString)
					argv = ['--program', self.program,  '--test-limit', '500']
					tuner.start(argv, cfg, self)
				except Exception, e:
					print "Exception occured : %s"%e
					traceback.print_exc()
					data = raw_input ( "Press Keyboard to exit..." )
					break;
					
			else:
				print "###Invalid data received. Please check...:" , data

	def sendmsg(self, msg):
		if not msg.endswith("\n"):
			msg = msg + "\n"
		self.socket.send(msg)

	def recvmsg(self):
		data = self.file.readline()
		if not data:
			print "Socket closed...."
			data = raw_input ( "Press Keyboard to exit..." )
			self.close()
			sys.exit(1)
		else:
			return data

	def close(self):
		self.socket.close()
		print "Socket has been closed successfully"
		
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('port', help='TCP port number', type=int)
	args = parser.parse_args()
	s = streamJit(args.port)
	s.listen()
	s.run()
	s.close()
		
