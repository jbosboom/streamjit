#!../../venv/bin/python
import argparse
import logging
import json
import sjparameters
import configuration
from streamjit import streamJit

import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (ConfigurationManipulator, IntegerParameter, FloatParameter)
from opentuner.measurement import MeasurementInterface
from opentuner.measurement.inputmanager import FixedInputManager
from opentuner.tuningrunmain import TuningRunMain
from opentuner.search.objective import MinimizeTime

class StreamJitMI(MeasurementInterface):
	''' Measurement Interface for tunning a StreamJit application'''
	def __init__(self, args, con, ss):
		super(StreamJitMI, self).__init__(args = args, program = args.program)
		self.connection = con
		self.sdk = ss
		self.trycount = 0

	def run(self, desired_result, input, limit):
		self.trycount = self.trycount + 1
		print "Try count = %d"%self.trycount
		cfg = desired_result.configuration.data
		print cfg
		self.sdk.sendmsg("%s\n"%cfg)
		msg = self.sdk.recvmsg()
		exetime = float(msg)
		print "Execution time is %f"%exetime
		return opentuner.resultsdb.models.Result(time=exetime)

def main(args, cfg, con, ss):
	logging.basicConfig(level=logging.INFO)
	manipulator = ConfigurationManipulator()

	print cfg
	params = cfg.getAllParameters()
	print params
	for key in params.keys():
  		manipulator.add_parameter(cfg.getParameter(key))
	
	m = TuningRunMain(manipulator,
                    StreamJitMI(args, con, ss),
                    FixedInputManager(),
                    MinimizeTime(),
                    args)
	m.main()

def start(argv, cfg, con, ss):
	log = logging.getLogger(__name__)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())

	parser.add_argument('--program', help='Name of the StreamJit application')
	
	args = parser.parse_args(argv)	

	if not args.database:
    		args.database = 'sqlite:///' + args.program + '.db'

	main(args, cfg, con, ss)
