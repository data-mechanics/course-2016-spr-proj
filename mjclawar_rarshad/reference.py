import os
from prov.model import ProvDocument

api_token_file = os.path.join(os.path.dirname(__file__), 'api_token.txt')
assert os.path.isfile(api_token_file)

with open(api_token_file) as f:
    api_token = f.read()

assert isinstance(api_token, str)

provenance_file = os.path.join(os.path.dirname(__file__), 'provenance.json')


def load_provenance_json():
    assert os.path.isfile(provenance_file)
    provenance_document = ProvDocument.deserialize(provenance_file)
    assert isinstance(provenance_document, ProvDocument)

    return provenance_document
