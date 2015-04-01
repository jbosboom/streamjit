#!../../venv/bin/python
import argparse
import logging
import json
import sjparameters
import configuration
import streamjit
import sys

import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (ConfigurationManipulator, IntegerParameter, FloatParameter)
from opentuner.measurement import MeasurementInterface
from opentuner.measurement.inputmanager import FixedInputManager
from opentuner.tuningrunmain import TuningRunMain
from opentuner.search.objective import MinimizeTime

class StreamJitMI(MeasurementInterface):
	''' Measurement Interface for tunning a StreamJit application'''
	def __init__(self, args, configuration, connection, manipulator, inputmanager, objective):
		super(StreamJitMI, self).__init__(args = args, program_name = args.program, manipulator = manipulator, input_manager = inputmanager, objective = objective)
		self.connection = connection
		self.trycount = 0
		self.config = configuration

	def run(self, desired_result, input, limit):
		self.trycount = self.trycount + 1
		print self.trycount

		cfg_data = desired_result.configuration.data
		#self.niceprint(cfg_data)
		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(cfg_data)
		self.connection.sendmsg(self.config.toJSON())

		msg = self.connection.recvmsg()
		if (msg == "exit\n"):
			#data = raw_input ( "exit cmd received. Press Keyboard to exit..." )
			self.connection.close()
			sys.exit(1)
		exetime = float(msg)
		if exetime < 0:
			print "Error in execution"
			return opentuner.resultsdb.models.Result(state='ERROR', time=float('inf'))
		else:	
			print "Execution time is %f"%exetime
			return opentuner.resultsdb.models.Result(time=exetime)

	def niceprint(self, cfg):
		print "\n--------------------------------------------------"
		print self.trycount
		for key in cfg.keys():
			print "%s - %s"%(key, cfg[key])

	def program_name(self):
		return self.args.program

	def program_version(self):
		return "1.0"

	def save_final_config(self, configuration):
		'''called at the end of autotuning with the best resultsdb.models.Configuration'''
		cfg_data = configuration.data
		print "Final configuration", cfg_data
		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(cfg_data)

		self.connection.sendmsg("Completed")
		self.connection.sendmsg(self.config.toJSON())
		self.connection.close()
		sys.exit(0)


def main(args, cfg, connection):
	logging.basicConfig(level=logging.INFO)
	manipulator = ConfigurationManipulator()

	#print "\nFeature variables...."
	for p in cfg.getAllParameters().values():
		manipulator.add_parameter(p)
	
	mi = StreamJitMI(args, cfg, connection, manipulator, FixedInputManager(),
                    MinimizeTime())

	m = TuningRunMain(mi, args)
	m.main()

def start(argv, cfg, connection):
	log = logging.getLogger(__name__)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())

	parser.add_argument('--program', help='Name of the StreamJit application')
	
	args = parser.parse_args(argv)

	if not args.database:
    		args.database = 'sqlite:///' + args.program + '.db'

	main(args, cfg, connection)
