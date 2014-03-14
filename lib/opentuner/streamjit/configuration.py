import json
import sjparameters

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
		# work around 'class' being reserved
		args['javaClass'] = args['class']
		del args['class']
		inst = class_(**args)
		return inst
	return d

class SJJSONEncoder(json.JSONEncoder):
	def default(self, o):
		if hasattr(o, 'json_replacement'):
			d = {'__class__':o.__class__.__name__, '__module__':o.__module__}
			d.update(o.json_replacement())
			return d
		else:
			return json.JSONEncoder.default(self, o)

def getConfiguration(cfgString):
	return json.loads(cfgString, object_hook=dict_to_object)

class Configuration:
	def __init__(self, params, subconfigs, extraData, javaClass):
		self.params = params
		self.subconfigs = subconfigs
		# Extra data is a mapping of string keys to 2-element lists, where the
		# first element is the Java class name and the second element is the
		# data itself.  This is required to avoid ambiguity when deserializing
		# on the Java side.
		self.extraData = extraData
		self.javaClass = javaClass

	def addParameter(self, id, param):
		self.params.update({id:param})

	def getParameter(self, id):
		return self.params[id]

	def deleteParameter(self, id):
		del self.params[id]

	def addSubConfiguration(self, id, subconfig):
		self.subconfigs.update({id:subconfig})

	def getSubConfiguration(self, id):
		return self.subconfigs[id]

	def deleteSubConfiguration(self, id):
		del self.subconfigs[id]

	def getAllParameters(self):
		return self.params

	def extra_data_keys(self):
		return self.extraData.keys()

	def get_extra_data(self, key):
		return self.extraData[key][1]

	def put_extra_data(self, key, data, javaClass):
		self.extraData[key] = [javaClass, data]

	def json_replacement(self):
		return {"params": self.params,
			"subconfigs": self.subconfigs,
			"extraData": self.extraData,
			"class": self.javaClass}

	def toJSON(self):
		return json.dumps(self, cls=SJJSONEncoder)
