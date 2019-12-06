import uuid

def gen_id():
	"""Returns a 16-byte Python bytes object"""
	return uuid.uuid4().bytes