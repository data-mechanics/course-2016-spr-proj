import os

api_token_file = os.path.join(os.path.dirname(__file__), 'api_token.txt')
assert os.path.isfile(api_token_file)

with open(api_token_file) as f:
    api_token = f.read()

assert isinstance(api_token, str)
