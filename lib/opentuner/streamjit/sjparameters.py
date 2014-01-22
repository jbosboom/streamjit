import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (IntegerParameter,
                                          FloatParameter, SwitchParameter)

class sjIntegerParameter(IntegerParameter):
	def __init__(self, name, min, max, value, javaClass = None, **kwargs):
		self.value = value
		self.javaClass = javaClass
		super(sjIntegerParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value

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

	def json_replacement(self):
		return {"name": self.name,
			"value": self.value,
			"universeType": self.universeType,
			"universe": self.universe,
			"class": self.javaClass}

if __name__ == '__main__':
	ip = IntegerParameter("suman", 2, 7)
	sjip = sjIntegerParameter("ss", 3, 56, 45)
	sjsw = sjSwitchParameter('sjswtch', 'java.lang.Integer', ['AAA', 'BBB', 'CCC', 'DDD'], 2, 'edu.mit.streamjit.impl.common.Configuration$SwitchParameter')
	print sjsw.getName()
	print sjsw.getUniverse()
	print sjsw.getValue()