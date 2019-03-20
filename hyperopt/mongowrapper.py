import pymongo
import pickle
import math
from bson.objectid import ObjectId


def recursiveApply(obj, funct):
	obj = funct(obj)
	if isinstance(obj, frozenset) or isinstance(obj, set):
		isFrozen = False
		if isinstance(obj, frozenset):
			isFrozen = True
		newObj = set()
		for current in obj:
			newObj.add(recursiveApply(current, funct))
		if isFrozen:
			newObj = frozenset(newObj)
		return newObj
	elif isinstance(obj, dict):
		for key in obj.keys():
			obj[key] = recursiveApply(obj[key], funct)
		return obj
	elif isinstance(obj, list):
		for i in range(len(obj)):
			obj[i] = recursiveApply(obj[i], funct)
		return obj
	elif isinstance(obj, tuple):
		newObj = []
		for current in obj:
			newObj.append(recursiveApply(current, funct))
		newObj = tuple(newObj)
		return newObj
	else:
		return obj

keyMappingToken = "__K_E_Y_M_A_P_P_I_N_G__"
# serializationToken = "__S_E_R_I_A_L__"

def getRandomStr(digitCount=10, withTimestamp=True):
    result = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(digitCount))
    if withTimestamp:
        timestamp = str(time.time())
        timestamp = timestamp.replace(".", "")
        result = result + "-" + timestamp
    return result

def intByteSize(n):
    if n == 0:
        return 1
    return int(math.log(n, 256)) + 1

def mongoSer(document, dollarEscape="__D_O_L_L_A_R__", pointEscape="_"):
	def __funct(obj):
		if isinstance(obj, dict):
			keyMapping = dict()
			for key in obj.keys():
				if isinstance(key, str) and "." in key:
					storableKey = key.replace(".", "_")
					if storableKey.startswith("$"):
						storableKey = "" + storableKey[1:]
					if storableKey in obj:
						storableKey = getRandomStr()
					keyMapping[storableKey] = key
			if len(keyMapping) == 0:
				return obj
			else:
				newObj = dict()
				alreadyStoredKeys = set()
				for storableKey, key in keyMapping.items():
					newObj[storableKey] = obj[key]
					alreadyStoredKeys.add(key)
				for key in obj.keys():
					if key not in alreadyStoredKeys:
						newObj[key] = obj[key]
				newObj[keyMappingToken] = pickle.dumps(keyMapping)
				return newObj
		elif obj is None or type(obj) in [float, bool, list, str, bytes, ObjectId]:
			return obj
		elif isinstance(obj, int):
			if intByteSize(obj) >= 8:
				return pickle.dumps(obj)
			else:
				return obj
		else:
			return pickle.dumps(obj)
	return recursiveApply(document, __funct)


def mongoDeser(document):
	def __funct(obj):
		if isinstance(obj, bytes):
			try:
				return pickle.loads(obj)
			except: pass
		elif isinstance(obj, dict):
			if keyMappingToken in obj:
				keyMapping = pickle.loads(obj[keyMappingToken])
				for storableKey, key in keyMapping.items():
					obj[key] = obj[storableKey]
					del obj[storableKey]
				del obj[keyMappingToken]
				return obj
		return obj
	return recursiveApply(document, __funct)



class SerCollection(pymongo.collection.Collection):
	# From <https://stackoverflow.com/questions/1443129/completely-wrap-an-object-in-python>
	def __init__(self, baseObject):
		self.__class__ = type(baseObject.__class__.__name__,
							  (self.__class__, baseObject.__class__),
							  {})
		self.__dict__ = baseObject.__dict__
	def insert(self, document, *args, **kwargs):
		document = mongoSer(document)
		return super().insert(document, *args, **kwargs)
	def ser_find(self, filter=None, **kwargs):
		filter = mongoSer(filter)
		docs = super().find(filter=filter, **kwargs)
		for doc in docs:
			yield mongoDeser(doc)
		# return [mongoDeser(doc) for doc in docs]
	def find_one(self, filter=None, **kwargs):
		filter = mongoSer(filter)
		doc = super().find_one(filter=filter, **kwargs)
		return mongoDeser(doc)
	def find_and_modify(self, query={}, update=None, **kwargs):
		query = mongoSer(query)
		doc = super().find_and_modify(query=query, update=update, **kwargs)
		return mongoDeser(doc)




if __name__ == '__main__':
	from collections import OrderedDict
	from enum import Enum
	from hpsklearn import any_classifier
	import hyperopt

	MY_ENUM = Enum("MY_ENUM", "a b")
	o = \
	{
		"id": "insert_test",
		"a": None,
		"x": 10000000000000000000000000000000000000000000000000000000000,
		"b": [2, {"C.T": 3}],
		"d": {"e", 6},
		"g.y": SerCollection,
		"h": MY_ENUM.a,
		"i": hyperopt.pyll.stochastic.sample(any_classifier("classifier")),
		"j": any_classifier("classifier"),
		# # "f": OrderedDict({"g": 7}),
	}

	def testRecursiveApply():
		def myFunct(o):
			if o is None:
				return 10000000
			if isinstance(o, int):
				return o + 10
			return o
		print(recursiveApply(o, myFunct))



	def testRecursiveApply():
		from pymongo import MongoClient
		client = MongoClient()
		client = MongoClient('localhost', 27017)
		db = client.test
		collection = db.col1
		# collection = SerCollection(collection)
		# collection.insert({"a.b": 1, "c": 2})
		# exit()

		try:
			collection.remove({"id": "insert_test"})
		except: pass
		print(o)
		gotException = False
		try:
			collection.insert(o)
		except:
			gotException = True
		assert gotException
		collection = SerCollection(collection)
		collection.insert(o)
		# exit()
		o2 = collection.find_one({"id": "insert_test"})
		del o2["_id"]
		print(o2)
		for key, value in o2.items():
			assert key in o
			assert type(value) == type(o[key])
		# assert o == o2
		# collection.insert({"a": MY_ENUM.a})
		# collection.insert(o)


	testRecursiveApply()

	# a = hyperopt.pyll.stochastic.sample(any_classifier("classifier"))
	# print(a)
	# print(type(a))
	# print()
	# a = pickle.dumps(a)
	# print(a)
	# print(type(a))
	# print()
	# a = pickle.loads(a)
	# print(a)
	# print(type(a))


	# import numpy as np
	# a.fit(np.array([np.array([1, 2]), np.array([3, 4])]), np.array([1, 2]))
	# print(a.predict([np.array([1, 2])]))


