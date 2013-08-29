import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (IntegerParameter,
                                          FloatParameter)

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

if __name__ == '__main__':
	ip = IntegerParameter("suman", 2, 7)
	sjip = sjIntegerParameter("ss", 3, 56, 45)
	print sjip.getValue()
