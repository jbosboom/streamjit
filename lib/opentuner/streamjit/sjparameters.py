import deps #fix sys.path
import random
import opentuner
from opentuner.search.manipulator import IntegerParameter, FloatParameter, SwitchParameter, PermutationParameter, ArrayParameter

class sjIntegerParameter(IntegerParameter):
	def __init__(self, name, min, max, value, javaClass = None, **kwargs):
		self.value = value
		self.javaClass = javaClass
		super(sjIntegerParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value

	def update_value_for_json(self, config):
		self.value = self.get_value(config)

	def json_replacement(self):
		(min, max) = self.legal_range(None)
		return {"name": self.name,
			"value": self.value,
			"min": min,
			"max": max,
			"class": self.javaClass}

class sjFloatParameter(FloatParameter):
	def __init__(self, name, min, max, value, javaClass = None, **kwargs):
		self.value = value
		self.javaClass = javaClass
		super(sjFloatParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value

	def update_value_for_json(self, config):
		self.value = self.get_value(config)

	def json_replacement(self):
		(min, max) = self.legal_range(None)
		return {"name": self.name,
			"value": self.value,
			"min": min,
			"max": max,
			"class": self.javaClass}

class sjSwitchParameter(SwitchParameter):
	def __init__(self, name, universeType, universe, value, javaClass = None, **kwargs):
		self.value = value
		self.javaClass = javaClass
		self.universeType = universeType
		self.universe = universe
		super(sjSwitchParameter, self).__init__(name, len(universe), **kwargs)

	def getValue(self):
		return self.universe[self.value]

	def getUniverse(self):
		return self.universe

	def getUniverseType(self):
		return self.universeType

	def update_value_for_json(self, config):
		self.value = self._get(config)

	def json_replacement(self):
		return {"name": self.name,
			"value": self.value,
			"universeType": self.universeType,
			"universe": self.universe,
			"class": self.javaClass}

class sjPermutationParameter(PermutationParameter):
	def __init__(self, name, universeType, universe, javaClass):
		self.javaClass = javaClass
		self.universeType = universeType
		self.universe = universe
		super(sjPermutationParameter, self).__init__(name, universe)

	def update_value_for_json(self, config):
		self.value = self._get(config)

	def json_replacement(self):
		return {"name": self.name,
			"universeType": self.universeType,
			"universe": self.universe,
			"class": self.javaClass}

class sjCompositionParameter(ArrayParameter):
	def __init__(self, name, values, javaClass):
		super(sjCompositionParameter, self).__init__(name, len(values), FloatParameter, 0.0, 1.0)
		self.values = values
		self.javaClass = javaClass

	def normalize(self, config):
		sum = 0
		for p in self.sub_parameters():
			sum += p.get_value(config)
		for p in self.sub_parameters():
			p.set_value(config, p.get_value(config)/sum)

	def seed_value(self):
		return [1.0/self.count for p in self.sub_parameters()]

	def add_difference(self, cfg_dst, scale, cfg_b, cfg_c):
		b_cores = len(self.nonzeroes(cfg_b))
		c_cores = len(self.nonzeroes(cfg_c))
		if b_cores > c_cores and self.add_core in self.manipulators(cfg_dst):
			self.add_core(cfg_dst)
		elif c_cores > b_cores and self.remove_core in self.manipulators(cfg_dst):
			self.remove_core(cfg_dst)
		else:
			pass

	def manipulators(self, config):
		manipulators = [self.randomize, self.shuffle_cores, self.equal_division]
		zeroes = len(self.zeroes(config))
		if zeroes > 0:
			manipulators.append(self.add_core)
		if zeroes < (self.count - 1):
			manipulators.append(self.remove_core)
		return manipulators

	def equal_division(self, config):
		for p in self.sub_parameters():
			p.set_value(config, 1.0/self.count)

	def shuffle_cores(self, config):
		values = [p.get_value(config) for p in self.sub_parameters()]
		random.shuffle(values)
		for (p, v) in zip(self.sub_parameters(), values):
			p.set_value(config, v)

	def add_core(self, config):
		"""Take a fraction of each non-zero core and move it to a zero core."""
		self.normalize(config)
		nonzeroes = self.nonzeroes(config)
		fraction = 1.0/(len(nonzeroes) + 1)
		random.choice(self.zeroes(config)).set_value(config, fraction)
		for p in nonzeroes:
			p.set_value(config, p.get_value(config) * (1.0 - fraction))

	def remove_core(self, config):
		"""Zero a core, equally distributing its value among other nonzero cores."""
		self.normalize(config)
		nonzeroes = self.nonzeroes(config)
		victim = random.choice(nonzeroes)
		v = victim.get_value(config)
		victim.set_value(config, 0.0)
		for p in nonzeroes:
			if p is not victim:
				p.set_value(config, p.get_value(config) + v/(len(nonzeroes)-1))

	def zeroes(self, config):
		return [p for p in self.sub_parameters() if p.get_value(config) == 0]

	def nonzeroes(self, config):
		return [p for p in self.sub_parameters() if p.get_value(config) > 0]

	def update_value_for_json(self, config):
		for (i, p) in zip(xrange(self.count), self.sub_parameters()):
			self.values[i] = p.get_value(config)

	def json_replacement(self):
		return {"name": self.name,
			"values": self.values,
			"class": self.javaClass}

if __name__ == '__main__':
	ip = IntegerParameter("suman", 2, 7)
	sjip = sjIntegerParameter("ss", 3, 56, 45)
	sjsw = sjSwitchParameter('sjswtch', 'java.lang.Integer', ['AAA', 'BBB', 'CCC', 'DDD'], 2, 'edu.mit.streamjit.impl.common.Configuration$SwitchParameter')
	print sjsw.getName()
	print sjsw.getUniverse()
	print sjsw.getValue()