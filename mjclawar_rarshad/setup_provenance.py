from mjclawar_rarshad.reference import provenance_document, provenance_file


def setup_provenance():
    # TODO don't use protected _namespaces
    if 'people' not in provenance_document._namespaces:
        provenance_document.add_namespace('people', 'https://github.com/mjclawar')

    provenance_document.serialize(provenance_file)

if __name__ == '__main__':
    setup_provenance()
    print('Done!')
