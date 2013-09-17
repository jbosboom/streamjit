import deps #fix sys.path
from opentuner.search.manipulator import (IntegerParameter, SwitchParameter, BooleanParameter, PowerOfTwoParameter)
import abc

class jvmParameter:
	@abc.abstractmethod
	def getValue(self):
		raise NotImplementedError

	@abc.abstractmethod
	def setValue(self, value):
		raise NotImplementedError

	def getCommand(self):
		return self.cmdFormat%self.getValue()

class jvmIntegerParameter(IntegerParameter, jvmParameter):
	def __init__(self, name, min, max,value, cmdFormat, **kwargs):
		self.value = value
		self.cmdFormat = cmdFormat	
		super(jvmIntegerParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value

	def setValue(self, value):
		self.value = value

class jvmSwitchParameter(SwitchParameter, jvmParameter):
	def __init__(self, name, universe,value, cmdFormat, **kwargs):
		self.value = value
		self.cmdFormat = cmdFormat
		self.universe = universe
		super(jvmSwitchParameter, self).__init__(name, len(universe), **kwargs)

	def getValue(self):
		return self.universe[self.value]

	def getUniverse(self):
		return self.universe

	def setValue(self, value):
		self.value = value

class jvmFlag(BooleanParameter, jvmParameter):
	def __init__(self, name,cmdFormat, **kwargs):
		self.cmdFormat = cmdFormat
		self.value = False
		super(jvmFlag, self).__init__(name, **kwargs)

	def setValue(self, value):
		self.value = value

	def getValue(self):
		return self.value

	def getCommand(self):
		if self.value:
			return self.cmdFormat
		else:
			return ''

class jvmPowerOfTwoParameter(PowerOfTwoParameter, jvmParameter):
	def __init__(self, name, min, max, cmdFormat, **kwargs):
		self.value = min
		self.cmdFormat = cmdFormat	
		super(jvmPowerOfTwoParameter, self).__init__(name, min, max, **kwargs)

	def getValue(self):
		return self.value

	def setValue(self, value):
		self.value = value

if __name__ == '__main__':
	xmx = jvmIntegerParameter("maxHeapSize", 3, 56, 45, " -Xmx%dm")
	gc = jvmSwitchParameter("GC",['UseSerialGC','UseParallelGC','UseParallelOldGC','UseConcMarkSweepGc'],2, " -XX:+%s")
	print xmx.getCommand()
	print gc.getCommand()
	gc.setValue(0)
	print gc.getCommand()

	xmx.setValue(56)
	print xmx.getCommand()

	useNUMA = jvmFlag("useNUMA", " -XX:+UseNUMA")
	#useNUMA.setValue(True)
	print useNUMA.getCommand()

	kk = jvmPowerOfTwoParameter("mem", 2, 1024, " -Xmx%dm")
	print kk.getCommand()
	
