#!/usr/bin/python
import argparse
import logging
import json
import sjparameters
import sjtechniques
import configuration
import streamjit
import sys
import subprocess
import tempfile
import os
import re
import time

import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (ConfigurationManipulator, IntegerParameter, FloatParameter)
from opentuner.measurement import MeasurementInterface
from opentuner.measurement.inputmanager import FixedInputManager
from opentuner.tuningrunmain import TuningRunMain
from opentuner.search.objective import MinimizeTime
from jvmparameters import *

class StreamJITMI(MeasurementInterface):
	def __init__(self, args, configuration, jvm_options, manipulator, inputmanager, objective):
		args.technique = ['StreamJITBandit']
		super(StreamJITMI, self).__init__(args = args, program_name = args.program, manipulator = manipulator, input_manager = inputmanager, objective = objective)
		self.program = args.program
		self.timestamp = args.timestamp
		self.config = configuration
		self.config.put_extra_data("benchmark", self.program, "java.lang.String")
		self.jvm_options = jvm_options

	def run(self, desired_result, input, limit):
		cfg_data = desired_result.configuration.data
		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(cfg_data)
		jvm_args = self.jvm_args_to_list(cfg_data)

		with tempfile.NamedTemporaryFile() as f:
			f.write(self.config.toJSON())
			f.flush()
			#TODO: use limit to time out earlier -- note will need Java-side changes
			#because we only care about compile time.
			(stdout, stderr) = call_java(jvm_args, "edu.mit.streamjit.tuner.RunApp2", ['@' + f.name])

		# cope with Java exceptions and JVM crashes
		try:
			if len(stderr) > 0 or len(stdout) == 0:
				raise ValueError
			stdout = stdout.strip(" \t\n\r")
			match = re.match(r"\d+/\d+/\d+/(\d+)#", stdout)
			if not match:
				raise ValueError
			time = float(match.group(1))
			print stdout
			return opentuner.resultsdb.models.Result(state='OK', time=time)
		except ValueError:
			print stderr.strip(" \t\n\r")
			if "TIMED OUT" in stderr:
				return opentuner.resultsdb.models.Result(state='TIMEOUT', time=float('inf'))
			else:
				with tempfile.NamedTemporaryFile(dir=os.getcwd(), prefix="error-{0}-{1}-".format(self.timestamp, self.program),
						suffix=".cfg", delete=False) as f:
					f.write(self.config.toJSON())
					f.write("\n")
					f.writelines(jvm_args)
					f.write("\n")
					f.write(stderr)
					if len(stdout) == 0:
						f.write("[stdout was empty]")
					f.flush()
				return opentuner.resultsdb.models.Result(state='ERROR', time=float('inf'))

	def save_final_config(self, config):
		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(config.data)
		with open("{0}-{1}.cfg".format(self.program, self.timestamp), 'w+') as f:
			f.write(self.config.toJSON())
			f.write("\n")
			f.write(" ".join(self.jvm_args_to_list(config.data)))
			f.flush()

	def jvm_args_to_list(self, cfg_data):
		jvm_args = []
		for key in self.jvm_options.keys():
			self.jvm_options.get(key).setValue(cfg_data[key])
			cmd = self.jvm_options.get(key).getCommand()
			if len(cmd) > 0:
				jvm_args.append(cmd)
		return jvm_args

class StreamJITConfigurationManipulator(ConfigurationManipulator):
	def __init__(self, config):
		super(StreamJITConfigurationManipulator, self).__init__()
		self.config = config
		self.allocationParams = dict()
		for k in self.config.extra_data_keys():
			match = re.match(r"AllocationParamNames(\d+)", k)
			if match:
				self.allocationParams[match.group(1)] = self.config.get_extra_data(k)

	def normalize(self, cfg_data):
		super(StreamJITConfigurationManipulator, self).normalize(cfg_data)
		#if we didn't get info, don't normalize
		if not self.allocationParams:
			return

		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(cfg_data)
		self.config.put_extra_data("reportFusion", True, "java.lang.Boolean")
		with tempfile.NamedTemporaryFile() as f:
			f.write(self.config.toJSON())
			f.flush()
			(stdout, stderr) = call_java([], "edu.mit.streamjit.tuner.RunApp2", ['@' + f.name])
		# don't disrupt other users of the config object
		self.config.put_extra_data("reportFusion", False, "java.lang.Boolean")

		parameters = self.parameters_dict(cfg_data)
		for group in stdout.splitlines():
			constituents = group.split()
			electeeParams = self.allocationParams[constituents[0]]
			for loser in constituents[1:]:
				for (epn, lpn) in zip(electeeParams, self.allocationParams[loser]):
					ep = parameters[epn]
					lp = parameters[lpn]
					lp._set(cfg_data, ep._get(cfg_data))

# Calls Java.  Returns a 2-tuple (stdout, stderr).
def call_java(jvmArgs, mainClass, args):
	#TODO: find a better place for these system-specific constants
	#the path to the Java executable, or "java" to use system's default
	javaPath = "java"
	#the classpath, suitable as the value of the '-cp' java argument
	javaClassPath = "build/jar/streamjit.jar:lib/asm.jar:lib/bridj.jar:lib/bytecodelib.jar:lib/guava.jar:lib/javax.json.jar:lib/joptsimple.jar:lib/sqlitejdbc.jar"

	finalArgs = [javaPath, "-cp", javaClassPath] + jvmArgs + [mainClass] + args
	return subprocess.Popen(finalArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

def make_jvm_options():
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
	compileThreshold = jvmIntegerParameter("compileThreshold", 50, 1000, 300, "-XX:CompileThreshold=%d")
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

	# options controlling inlining
	clipInlining = jvmFlag("clipInlining", "-XX:-ClipInlining") # on by default, we turn it off
	freqInlineSize = jvmIntegerParameter("freqInlineSize", 100, 10000, 325, "-XX:FreqInlineSize=%d")
	inlineSmallCode = jvmIntegerParameter("inlineSmallCode", 500, 10000, 1000, "-XX:InlineSmallCode=%d")
	maxInlineSize = jvmIntegerParameter("maxInlineSize", 20, 1000, 35, "-XX:MaxInlineSize=%d")
	maxInlineLevel = jvmIntegerParameter("maxInlineLevel", 5, 20, 9, "-XX:MaxInlineLevel=%d")

	eliminateArrays = jvmIntegerParameter("eliminateAllocationArraySizeLimit", 64, 2048, 64, "-XX:EliminateAllocationArraySizeLimit=%d")
	useNuma = jvmFlag("useNuma", "-XX:+UseNUMA")
	bindGCTaskThreadsToCPUs = jvmFlag("bindGCTaskThreadsToCPUs", "-XX:+BindGCTaskThreadsToCPUs")

	enabledJvmOptions = [aggressiveOpts, compileThreshold, clipInlining, freqInlineSize,
		maxInlineSize, maxInlineLevel, eliminateArrays, useNuma, bindGCTaskThreadsToCPUs]
	return {x.name:x for x in enabledJvmOptions}

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())
	parser.add_argument('--program', help='StreamJIT benchmark to tune (with first input)')
	parser.add_argument('--timestamp', help='timestamp to use for final config/errors',
		default=time.strftime('%Y%m%d-%H%M%S'))
	args = parser.parse_args()
	(cfg_json, error_str) = call_java([], "edu.mit.streamjit.tuner.ConfigGenerator2",
		["edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory", args.program])
	if len(error_str) > 0:
		sys.exit("Getting config JSON: "+error_str)
	cfg = configuration.getConfiguration(cfg_json)
	jvm_options = make_jvm_options();

	manipulator = StreamJITConfigurationManipulator(cfg)
	for p in cfg.getAllParameters().values() + jvm_options.values():
		manipulator.add_parameter(p)

	# create seed configurations
	seed_multipliers = [1024, 4096, 128]
	seed_configs = []
	for m in seed_multipliers:
		seed_config = manipulator.seed_config()
		for p in cfg.getAllParameters().values() + jvm_options.values():
			if isinstance(p, sjparameters.sjCompositionParameter):
				p.equal_division(seed_config)
			elif isinstance(p, sjparameters.sjPermutationParameter):
				pass #p.set_value(config, p.seed_value())
			else:
				seed_config[p.name] = p.value
		seed_config['multiplier'] = m
		seed_configs.append(seed_config)

	# The default bandit, plus our custom techniques.
	from opentuner.search import technique, bandittechniques, differentialevolution, evolutionarytechniques, simplextechniques
	technique.register(bandittechniques.AUCBanditMetaTechnique([
			sjtechniques.FixedTechnique(seed_configs),
			differentialevolution.DifferentialEvolutionAlt(),
			evolutionarytechniques.UniformGreedyMutation(),
			evolutionarytechniques.NormalGreedyMutation(mutation_rate=0.3),
			simplextechniques.RandomNelderMead(),
			sjtechniques.ForceRemove(),
			sjtechniques.ForceFuse(),
			sjtechniques.ForceUnbox(),
			sjtechniques.ForceEqualDivision(),
			sjtechniques.CrossSocketBeforeHyperthreadingAffinity(),
		], name = "StreamJITBandit"))

	mi = StreamJITMI(args, cfg, jvm_options, manipulator, FixedInputManager(), MinimizeTime())
	m = TuningRunMain(mi, args)
	m.main()
