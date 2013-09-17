#!../../venv/bin/python
import argparse
import logging
import json
import sjparameters
import configuration
import streamjit
import sys
import subprocess
import sqlite3
import traceback

import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (ConfigurationManipulator, IntegerParameter, FloatParameter)
from opentuner.measurement import MeasurementInterface
from opentuner.measurement.inputmanager import FixedInputManager
from opentuner.tuningrunmain import TuningRunMain
from opentuner.search.objective import MinimizeTime
from jvmparameters import *

class StreamJitMI(MeasurementInterface):
	''' Measurement Interface for tunning a StreamJit application'''
	def __init__(self, args, jvm, manipulator, inputmanager, objective):
		super(StreamJitMI, self).__init__(args = args, program_name = args.program, manipulator = manipulator, input_manager = inputmanager, objective = objective)
		self.trycount = 0
		self.jvm = jvm
		self.program = args.program
		try:
			self.con = sqlite3.connect('sj' + args.program + '.db')
			c = self.con.cursor()
			c.execute("drop table if exists results")
			c.execute('''CREATE TABLE results ( Round int, JVMOption text, SJConfig text, Exectime real)''')
			self.con.commit()
		except Exception, e:
			print "Exception occured : %s"%e
			traceback.print_exc()
			data = raw_input ( "Press Keyboard to exit..." )

	def run(self, desired_result, input, limit):
		print 'RUN.....................'
		self.trycount = self.trycount + 1
		cfg = dict.copy(desired_result.configuration.data)
		#self.niceprint(cfg)
		commandStr = ''
		args = ["java"]
		for key in self.jvm.keys():
			#print "\t", key
  			val = cfg[key]
			#del cfg[key]
			self.jvm.get(key).setValue(val)
			cmd = self.jvm.get(key).getCommand()
			commandStr += cmd
			args.append(cmd)
				
		cur = self.con.cursor()
		query = 'INSERT INTO results VALUES (%d,"%s","%s", "%f")'%(self.trycount, commandStr, cfg, -1)
		cur.execute(query)
		self.con.commit()
		
		print commandStr

		args.append("-jar")
		args.append("RunApp.jar")
		args.append("%s"%self.program)
		args.append("%d"%self.trycount)

		#p = subprocess.Popen(["java",'%s'%commandStr, "-jar","RunApp.jar", "%s"%self.program, "%d"%self.trycount])
		p = subprocess.Popen(args)
		p.wait()		
		
		cur.execute('SELECT exectime FROM results WHERE round=%d'%self.trycount)
		row = cur.fetchone()
		time = row[0]

		print time
		exetime = float(time)
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
		self.trycount = self.trycount + 1
		cfg = dict.copy(configuration.data)
		commandStr = ''
		args = ["java"]
		for key in self.jvm.keys():
  			val = cfg[key]
			self.jvm.get(key).setValue(val)
			cmd = self.jvm.get(key).getCommand()
			commandStr += cmd
			args.append(cmd)
				
		cur = self.con.cursor()
		query = 'INSERT INTO results VALUES (%d,"%s","%s", 0)'%(self.trycount, commandStr, cfg)
		cur.execute(query)
		self.con.commit()
		
		print commandStr

		args.append("-jar")
		args.append("RunApp.jar")
		args.append("%s"%self.program)
		args.append("%d"%self.trycount)

		#p = subprocess.Popen(["java",'%s'%commandStr, "-jar","RunApp.jar", "%s"%self.program, "%d"%self.trycount])
		p = subprocess.Popen(args)
		p.wait()		
		
		cur.execute('SELECT exectime FROM results WHERE round=%d'%self.trycount)
		row = cur.fetchone()
		time = row[0]

		print time

		conn = sqlite3.connect('streamjit.db')
		cursor = conn.cursor()
		query = 'INSERT INTO FinalResult VALUES ("%s","%s","%s",%d, %f)'%(self.program, commandStr, cfg, self.trycount, time)
		cursor.execute(query)
		conn.commit()

def main(args, cfg, jvm):
	logging.basicConfig(level=logging.INFO)
	manipulator = ConfigurationManipulator()

	params = dict(cfg.items() + jvm.items())
	#print "\nFeature variables...."
	for key in params.keys():
		#print "\t", key
  		manipulator.add_parameter(params.get(key))
	
	mi = StreamJitMI(args,jvm, manipulator, FixedInputManager(),
                    MinimizeTime())

	m = TuningRunMain(mi, args)
	m.main()

def start(program):
	log = logging.getLogger(__name__)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())
	parser.add_argument('--program', help='Name of the StreamJit application')

	argv = ['--program', program,  '--test-limit', '1']
	args = parser.parse_args(argv)

	if not args.database:
    		args.database = 'sqlite:///' + program + '.db'

	try: 
		conn = sqlite3.connect('streamjit.db')
		c = conn.cursor()
		query = 'SELECT configuration FROM apps WHERE name="%s"'%program
		c.execute(query)
		row = c.fetchone()
		cfgString = row[0]
		cfg = configuration.getConfiguration(cfgString)
		cfgparams = cfg.getAllParameters()
	except Exception, e:
		print 'Exception occured'
		traceback.print_exc()
		data = raw_input ( "Press Keyboard to exit..." )
		

	maxHeap = jvmPowerOfTwoParameter("maxHeap", 512, 2048, "-Xmx%dm")
	gc = jvmSwitchParameter("GC",['UseSerialGC','UseParallelGC','UseParallelOldGC','UseConcMarkSweepGC'],2, "-XX:+%s")
	

	jvmparams = {"GC":gc, "maxHeap":maxHeap}

	main(args, cfgparams, jvmparams)

if __name__ == '__main__':
	start('ChannelVocoder 4, 64')
	start('FMRadio 11, 64')
	start('BitonicSort (N = 4, asc)')
	
