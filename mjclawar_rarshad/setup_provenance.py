from prov.model import ProvDocument

from mjclawar_rarshad import mcra_structures as mcras
from mjclawar_rarshad import reference, database_helpers


def load_provenance_json():
    # TODO load from repo
    provenance_document = ProvDocument.deserialize(reference.provenance_file)
    return provenance_document


def initialize_provenance():
    prov_doc = ProvDocument()
    prov_doc.add_namespace(mcras.BDP_NAMESPACE.name, mcras.BDP_NAMESPACE.link)
    prov_doc.add_namespace(mcras.ALG_NAMESPACE.name, mcras.ALG_NAMESPACE.link)
    prov_doc.add_namespace(mcras.DAT_NAMESPACE.name, mcras.DAT_NAMESPACE.link)
    prov_doc.add_namespace(mcras.LOG_NAMESPACE.name, mcras.LOG_NAMESPACE.link)
    prov_doc.add_namespace(mcras.ONT_NAMESPACE.name, mcras.ONT_NAMESPACE.link)

    write_provenance_json(prov_doc)


def write_provenance_json(prov_doc):
    assert isinstance(prov_doc, ProvDocument)
    prov_doc.serialize(reference.provenance_file)


if __name__ == '__main__':
    initialize_provenance()
    print('Done!')
