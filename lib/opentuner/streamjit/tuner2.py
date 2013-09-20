#!/usr/bin/python
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
	def __init__(self, args, jvmOptions, manipulator, inputmanager, objective):
		super(StreamJitMI, self).__init__(args = args, program_name = args.program, manipulator = manipulator, input_manager = inputmanager, objective = objective)
		self.trycount = 0
		self.jvmOptions = jvmOptions
		self.program = args.program
		try:
			self.tunedataDB = sqlite3.connect('sj' + args.program + '.db')
			c = self.tunedataDB.cursor()
			c.execute("drop table if exists results")
			c.execute('''CREATE TABLE results ( Round int, JVMOption text, SJConfig text, Exectime real)''')
			c.execute('''CREATE TABLE if not exists exceptions (ExpMsg text, JVMOption text, SJConfig text)''')
			self.tunedataDB.commit()
		except Exception, e:
			print "Exception occured : %s"%e
			traceback.print_exc()
			data = raw_input ( "Press Keyboard to exit..." )

	def run(self, desired_result, input, limit):
		self.trycount = self.trycount + 1
		print '\n**********New Run - %d **********'%self.trycount
		cfg = dict.copy(desired_result.configuration.data)
		#self.niceprint(cfg)
		commandStr = ''
		args = ["java"]
		for key in self.jvmOptions.keys():
			#print "\t", key
  			val = cfg[key]
			self.jvmOptions.get(key).setValue(val)
			cmd = self.jvmOptions.get(key).getCommand()
			commandStr += cmd
			args.append(cmd)
				
		cur = self.tunedataDB.cursor()
		query = 'INSERT INTO results VALUES (%d,"%s","%s", "%f")'%(self.trycount, commandStr, cfg, -1)
		cur.execute(query)
		self.tunedataDB.commit()
		
		print commandStr

		args.append("-jar")
		args.append("RunApp.jar")
		args.append("%s"%self.program)
		args.append("%d"%self.trycount)

		#p = subprocess.Popen(["java",'%s'%commandStr, "-jar","RunApp.jar", "%s"%self.program, "%d"%self.trycount])
		p = subprocess.Popen(args, stderr=subprocess.PIPE)
		p.wait()		
		out, err = p.communicate()
		# Do not comment following 'pring err' line. Commenting will cause deadlog due to std error buffer become full.
		print err
		if err.find("Exception") > -1:
			print "\033[31;1mException Found\033[0m"
			cur = self.tunedataDB.cursor()
			str1 = str(commandStr)
			str2 = str(cfg)
			cur.execute('INSERT INTO exceptions VALUES (?,?,?)', (err, str1, str2))
			self.tunedataDB.commit()
			return opentuner.resultsdb.models.Result(state='ERROR', time=float('inf'))

		cur.execute('SELECT exectime FROM results WHERE round=%d'%self.trycount)
		row = cur.fetchone()
		time = row[0]

		exetime = float(time)
		if exetime < 0:
			print "\033[31;1mError in execution\033[0m"
			return opentuner.resultsdb.models.Result(state='ERROR', time=float('inf'))
		else:	
			print "\033[32;1mExecution time is %f ms\033[0m"%exetime
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
		for key in self.jvmOptions.keys():
  			val = cfg[key]
			self.jvmOptions.get(key).setValue(val)
			cmd = self.jvmOptions.get(key).getCommand()
			commandStr += cmd
			args.append(cmd)
				
		cur = self.tunedataDB.cursor()
		query = 'INSERT INTO results VALUES (%d,"%s","%s", 0)'%(self.trycount, commandStr, cfg)
		cur.execute(query)
		self.tunedataDB.commit()
		
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

		conn = sqlite3.connect('streamjit.db', 100)
		cur = conn.cursor()
		query = 'INSERT INTO FinalResult VALUES ("%s","%s","%s",%d, %f)'%(self.program, commandStr, cfg, self.trycount, time)
		cur.execute(query)
		conn.commit()

def main(args, cfg, jvmOptions):
	logging.basicConfig(level=logging.INFO)
	manipulator = ConfigurationManipulator()

	params = dict(cfg.items() + jvmOptions.items())
	#print "\nFeature variables...."
	for key in params.keys():
		#print "\t", key
  		manipulator.add_parameter(params.get(key))
	
	mi = StreamJitMI(args,jvmOptions, manipulator, FixedInputManager(),
                    MinimizeTime())

	m = TuningRunMain(mi, args)
	m.main()

def start(program):
	log = logging.getLogger(__name__)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())
	parser.add_argument('--program', help='Name of the StreamJit application')

	argv = ['--program', program,  '--test-limit', '2000']
	args = parser.parse_args(argv)

	if not args.database:
    		args.database = 'sqlite:///' + program + '.db'

	try: 
		conn = sqlite3.connect('streamjit.db')
		c = conn.cursor()
		query = 'SELECT configuration FROM apps WHERE name="%s"'%program
		c.execute(query)
		row = c.fetchone()
		if not row:
			data = raw_input ( "No entry found with name = %s \nPlease press anykey to exit"%program )
			sys.exit(1)
		cfgString = row[0]
		cfg = configuration.getConfiguration(cfgString)
		cfgparams = cfg.getAllParameters()
	except Exception, e:
		print 'Exception occured'
		traceback.print_exc()
		data = raw_input ( "Press Keyboard to exit..." )
		

	maxHeap = jvmPowerOfTwoParameter("maxHeap", 512, 2048, "-Xmx%dm")

	# Garbage First (G1) Garbage Collection Options
	gc = jvmSwitchParameter("gc",['UseSerialGC','UseParallelGC','UseParallelOldGC','UseConcMarkSweepGC'],2, "-XX:+%s")
	newRatio = jvmIntegerParameter("newRatio", 1, 50, 8, "-XX:NewRatio=%d")
	maxPermSize = jvmPowerOfTwoParameter("maxPermSize", 8, 2048, "-XX:MaxPermSize=%dm")
	maxGCPauseMillis = jvmIntegerParameter("maxGCPauseMillis", 50, 5000, 100, "-XX:MaxGCPauseMillis=%d")
	survivorRatio = jvmIntegerParameter("survivorRatio", 2, 100, 8, "-XX:SurvivorRatio=%d")
	parallelGCThreads = jvmIntegerParameter("parallelGCThreads", 2, 25, 4, "-XX:ParallelGCThreads=%d")
	concGCThreads = jvmIntegerParameter("concGCThreads", 2, 25, 4, "-XX:ConcGCThreads=%d")

	# Performance Options
	aggressiveOpts = jvmFlag("aggressiveOpts", "-XX:+AggressiveOpts")
	compileThreshold = jvmIntegerParameter("compileThreshold", 1000, 100000, 1500, "-XX:CompileThreshold=%d")
	largePageSizeInBytes = jvmPowerOfTwoParameter("largePageSizeInBytes", 2, 256, "-XX:LargePageSizeInBytes=%dm")
	maxHeapFreeRatio = jvmIntegerParameter("maxHeapFreeRatio", 10, 100, 70, "-XX:MaxHeapFreeRatio=%d")
	minHeapFreeRatio = jvmIntegerParameter("minHeapFreeRatio", 5, 100, 70, "-XX:MinHeapFreeRatio=%d")
	threadStackSize = jvmPowerOfTwoParameter("threadStackSize", 64, 8192, "-XX:ThreadStackSize=%d")
	useBiasedLocking = jvmFlag("useBiasedLocking", "-XX:+UseBiasedLocking")
	useFastAccessorMethods = jvmFlag("useFastAccessorMethods", "-XX:+UseFastAccessorMethods")
	useISM = jvmFlag("useISM", "-XX:-UseISM")
	useLargePages = jvmFlag("useLargePages", "-XX:+UseLargePages")
	useStringCache = jvmFlag("useStringCache", "-XX:+UseStringCache")
	allocatePrefetchLines = jvmIntegerParameter("allocatePrefetchLines", 1, 10, 1, "-XX:AllocatePrefetchLines=%d")
	allocatePrefetchStyle = jvmIntegerParameter("allocatePrefetchStyle", 0, 2, 1, "-XX:AllocatePrefetchStyle=%d")
	useCompressedStrings = jvmFlag("useCompressedStrings", "-XX:+UseCompressedStrings")
	optimizeStringConcat = jvmFlag("optimizeStringConcat", "-XX:+OptimizeStringConcat")

	#jvmOptions = {"gc":gc, "maxHeap":maxHeap, "newRatio":newRatio, "maxPermSize":maxPermSize, "maxGCPauseMillis":maxGCPauseMillis, "survivorRatio":survivorRatio, "parallelGCThreads":parallelGCThreads, "concGCThreads":concGCThreads, "aggressiveOpts":aggressiveOpts,  "compileThreshold":compileThreshold, "largePageSizeInBytes":largePageSizeInBytes, "maxHeapFreeRatio":maxHeapFreeRatio, "minHeapFreeRatio":minHeapFreeRatio, "threadStackSize":threadStackSize, "useBiasedLocking":useBiasedLocking, "useFastAccessorMethods":useFastAccessorMethods, "useISM":useISM, "useLargePages":useLargePages, "useStringCache":useStringCache, "allocatePrefetchLines":allocatePrefetchLines, "allocatePrefetchStyle":allocatePrefetchStyle, "useCompressedStrings":useCompressedStrings, "optimizeStringConcat":optimizeStringConcat}

	jvmOptions = {"gc":gc, "maxHeap":maxHeap, "newRatio":newRatio, "maxPermSize":maxPermSize, "maxGCPauseMillis":maxGCPauseMillis, "survivorRatio":survivorRatio, "parallelGCThreads":parallelGCThreads, "concGCThreads":concGCThreads, "aggressiveOpts":aggressiveOpts,  "compileThreshold":compileThreshold, "largePageSizeInBytes":largePageSizeInBytes, "threadStackSize":threadStackSize}

	main(args, cfgparams, jvmOptions)

if __name__ == '__main__':
	prgrms = []
	with sqlite3.connect('streamjit.db') as conn:
		c = conn.cursor()
		query = 'SELECT name FROM apps'
		for row in c.execute(query):
			prgrms.append(row[0])
		print "Following programs found for tuning..."
		for p in prgrms:
			print p

	for p in prgrms:
		print "************%s***************"%p
		start(p)

	#start('ChannelVocoder 4, 64')
	#start('FMRadio 11, 64')
	#start('BitonicSort (N = 4, asc)')
	
