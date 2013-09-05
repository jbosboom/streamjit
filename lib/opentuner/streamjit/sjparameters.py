import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (IntegerParameter,
                                          FloatParameter, SwitchParameter)

class sjIntegerParameter(IntegerParameter):
	def __init__(self, name, min, max,value, javaClassPath = None, **kwargs):
		self.value = value
		self.javaClassPath = javaClassPath	
		super(sjIntegerParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value	

	def getJavaClassPath(self):
		return self.javaClassPath

class sjFloatParameter(FloatParameter):
	def __init__(self, name, min, max,value, javaClassPath = None, **kwargs):
		self.value = value
		self.javaClassPath = javaClassPath	
		super(sjIntegerParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value	

	def getJavaClassPath(self):
		return self.javaClassPath

class sjSwitchParameter(SwitchParameter):
	def __init__(self, name, universeType, universe,value, javaClassPath = None, **kwargs):
		self.value = value
		self.javaClassPath = javaClassPath
		self.universeType = universeType
		self.universe = universe
		super(sjSwitchParameter, self).__init__(name, len(universe), **kwargs)

	def getValue(self):
		return self.value

	def getJavaClassPath(self):
		return self.javaClassPath

	def getUniverse(self):
		return self.universe

	def getUniverseType(self):
		return self.universeType

if __name__ == '__main__':
	ip = IntegerParameter("suman", 2, 7)
	sjip = sjIntegerParameter("ss", 3, 56, 45)
	sjsw = sjSwitchParameter('sjswtch', 'java.lang.Integer', [1, 2, 3, 4], 2, 'edu.mit.streamjit.impl.common.Configuration$SwitchParameter')
	print sjsw.getUniverse()
	print sjip.getValue()
