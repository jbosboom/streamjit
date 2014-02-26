import abc
import os
import subprocess
try:
	from xml import etree
except ImportError:
	try:
		import xml.etree.cElementTree as etree
	except ImportError:
		import xml.etree.ElementTree as etree
from opentuner.search import technique
from sjparameters import sjSwitchParameter, sjCompositionParameter

# base class for techniques that mutate the best configuration
class BestBasedTechnique(technique.SearchTechnique):
	__metaclass__ = abc.ABCMeta

	def __init__(self, *pargs, **kwargs):
		super(BestBasedTechnique, self).__init__(*pargs, **kwargs)

	def desired_configuration(self):
		for result in self.driver.results_query(objective_ordered = True):
			data = self.manipulator.copy(result.configuration.data)
			next_data = self.mutate(self.manipulator.parameters(data), data)
			next_cfg = self.driver.get_configuration(next_data)
			if next_cfg.id is not None: # don't resubmit
				continue
			return next_cfg
		return None

	@abc.abstractmethod
	def mutate(self, params, data):
		pass

class ForceTrue(BestBasedTechnique):
	def __init__(self, prefix, *pargs, **kwargs):
		super(ForceTrue, self).__init__(*pargs, **kwargs)
		self.prefix = prefix

	def mutate(self, params, data):
		for param in params:
			if isinstance(param, sjSwitchParameter) and param.name.startswith(self.prefix):
				param._set(data, 1)
		return data

class ForceRemove(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceRemove, self).__init__("remove", *pargs, **kwargs)

class ForceFuse(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceFuse, self).__init__("fuse", *pargs, **kwargs)

class ForceUnbox(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceUnbox, self).__init__("unbox", *pargs, **kwargs)

class ForceEqualDivision(BestBasedTechnique):
	def __init__(self):
		super(ForceEqualDivision, self).__init__()

	def mutate(self, params, data):
		for param in params:
			if isinstance(param, sjCompositionParameter):
				param.equal_division(data)
		return data

class CrossSocketBeforeHyperthreadingAffinity(BestBasedTechnique):
	def __init__(self):
		super(CrossSocketBeforeHyperthreadingAffinity, self).__init__()
		pid = os.getpid()
		lstopo_args = ["lstopo", "--of", "xml", "--pid", str(pid)]
		stdout, stderr = subprocess.Popen(lstopo_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
		if stderr:
			print "lstopo error", stderr
		root = etree.ElementTree.fromstring(stdout)
		all_cpus = [int(x.get('os_index')) for x in root.findall(".//object[type='PU']")]
		self.desired_perm = []
		while len(self.desired_perm) < len(all_cpus):
			for socket in root.findall(".//object[type='Socket']"):
				for core in socket.findall(".//object[type='Core']"):
					# Pick just one PU from this core each time we get here.
					for unit in core.findall(".//object[type='PU']"):
						index = int(unit.get('os_index'))
						if index not in self.desired_perm:
							self.desired_perm.append(index)
							break

	def mutate(self, params, data):
		for param in params:
			if param.name == "$affinity":
				legal = param.get_value(data)
				new_perm = [x for x in self.desired_perm if x in legal] + [x for x in legal if x not in self.desired_perm]
				param._set(data, new_perm)
		return data

# returns a list of fixed configurations in sequence
class FixedTechnique(technique.SearchTechnique):
	def __init__(self, config_data_list, *pargs, **kwargs):
		super(FixedTechnique, self).__init__(*pargs, **kwargs)
		self.config_data_list = config_data_list
		self.index = 0

	def desired_configuration(self):
		# OpenTuner should do this for us?!
		if not self.is_ready():
			return None
		cfg = self.driver.get_configuration(self.config_data_list[self.index])
		self.index = self.index + 1
		return cfg

	def is_ready(self):
		return self.index < len(self.config_data_list)
