import sys
import json

# get credential file
if len(sys.argv) < 2:	# no auth.json is given
	auth_file = input('Please enter the auth file: ')
else:
	auth_file = sys.argv[1]
auth = json.loads(open(auth_file).read())
