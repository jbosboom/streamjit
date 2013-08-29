import json
import sjparameters


def convert_to_builtin_type(obj):
    # Convert objects to a dictionary of their representation
    d = { '__class__':obj.__class__.__name__, 
          '__module__':obj.__module__,
          }
    d.update(obj.__dict__)
    return d
	
def dict_to_object(d):
    if '__class__' in d:
        class_name = d.pop('__class__')
        module_name = d.pop('__module__')
        module = __import__(module_name)
        #print 'MODULE:', module
        class_ = getattr(module, class_name)
        #print 'CLASS:', class_
        args = dict( (key.encode('ascii'), value) for key, value in d.items())
        #print 'INSTANCE ARGS:', args
        inst = class_(**args)		
    else:
        inst = d
    return inst

def getConfiguration(cfgString):
	return json.loads(cfgString, object_hook=dict_to_object)

class Configuration:
	def __init__(self, params, subconfigs,extraData,javaClassPath):
		self.params = params
		self.subconfigs = subconfigs
		self.javaClassPath = javaClassPath
		
	def addParameter(self, id, param):
		self.params.update({id:param})
		
	def getParameter(self, id):
		return self.params[id]
		
	def deleteParameter(self, id):
		del self.params[id]

	def getJavaClassPath(self):
		return self.javaClassPath
		
	def addSubConfiguration(self, id, subconfig):
		self.subconfigs.update({id:subconfig})
		
	def getSubConfiguration(self, id):
		return self.subconfigs[id]
		
	def deleteSubConfiguration(self, id):
		del self.subconfigs[id]

	def getAllParameters(self):
		return self.params

# To test the class		
if __name__ == '__main__':
	c = Configuration({},{}, 'no.1.class.path')
	subconfig = Configuration({},{},'no.sub1.class.path')
	ip = sjIntegerParameter('fooip', 3, 5, 6, 'no.2.class.path')
	subip = sjIntegerParameter('subip', 0, 15, 10, 'no.sub2.class.path')
	subconfig.addParameter('subip',subip)
	c.addParameter('foo', ip)
	c.addSubConfiguration('fstsubconfg', subconfig)
	print c.getSubConfiguration('fstsubconfg').getParameter('subip').getJavaClassPath()
	
