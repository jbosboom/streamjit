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
	def __init__(self, args, ss, manipulator, inputmanager, objective):
		super(StreamJitMI, self).__init__(args = args, program_name = args.program, manipulator = manipulator, input_manager = inputmanager, objective = objective)
		self.sdk = ss
		self.trycount = 0

	def run(self, desired_result, input, limit):
		self.trycount = self.trycount + 1
		cfg = desired_result.configuration.data
		self.niceprint(cfg)
		self.sdk.sendmsg("%s\n"%cfg)
		msg = self.sdk.recvmsg()
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
		cfg = configuration.data
		print "Final configuration", cfg
		self.sdk.sendmsg("Completed")
		self.sdk.sendmsg("%s\n"%cfg)
		self.sdk.close()
		sys.exit(0)


def main(args, cfg, ss):
	logging.basicConfig(level=logging.INFO)
	manipulator = ConfigurationManipulator()

	params = cfg.getAllParameters()
	print "\nFeature variables...."
	for key in params.keys():
		print "\t", key
  		manipulator.add_parameter(cfg.getParameter(key))
	
	mi = StreamJitMI(args, ss, manipulator, FixedInputManager(),
                    MinimizeTime())

	m = TuningRunMain(mi, args)
	m.main()

def start(argv, cfg, ss):
	log = logging.getLogger(__name__)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())

	parser.add_argument('--program', help='Name of the StreamJit application')
	
	args = parser.parse_args(argv)

	if not args.database:
    		args.database = 'sqlite:///' + args.program + '.db'

	main(args, cfg, ss)
