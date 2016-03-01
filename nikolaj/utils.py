import datetime

def timestamped(f):
	def wrap(*args, **kwargs):
		return datetime.datetime.now(), f(*args, **kwargs), datetime.datetime.now()
	return wrap
