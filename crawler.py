import requests
import rdflib


# stores the URIs for each containedItemClass to be harvested
URIS = []


def crawl_ldapi(register_uri):
    pass


def get_graph_from_uri(uri):
    r = requests.get(uri, headers={'Accept': 'text/turtle'})  # TODO: add more RDF format support
    rdf_text = r.content.decode('utf-8')
    g = rdflib.Graph()
    g.parse(data=rdf_text, format='turtle')

    return g


def get_contained_item_class_uris(g):
    q = '''
        PREFIX reg: <http://purl.org/linked-data/registry#>
        SELECT ?uri
        WHERE {                
            ?reg reg:containedItemClass ?containedItemClass .
            ?uri a ?containedItemClass .
        }
    '''
    for r in g.query(q):
        yield str(r['uri'])


def get_next_page_uri(g):
    q = '''
        PREFIX ldp: <http://www.w3.org/ns/ldp#>
        SELECT ?next
        WHERE {{
            ?page_uri xhv:next ?next .
        }}
    '''
    for r in g.query(q):
        return str(r['next'])

    return None


def crawl_register(register_uri):
    # start by loading the register URIs
    g = get_graph_from_uri(register_uri)

    # get the containedItemClass URIs from this Register/Page, regardless of if there are more Pages or not
    URIS.extend(get_contained_item_class_uris(g))
    print('Added {} URIs to the list from Page {}'.format(len(URIS), register_uri))

    # check to see if this register is paging
    q = '''
        PREFIX ldp: <http://www.w3.org/ns/ldp#>
        SELECT ?page_uri
        WHERE {{
            ?page_uri ldp:pageOf <{}> .
        }}
    '''.format(
        register_uri
    )
    for r in g.query(q):
        page_uri = str(r['page_uri'])

    # if this register has pages, paginate treating this as the first page
    if 'page_uri' in locals():
        # if there is another page, load that
        p = get_next_page_uri(g)
        while p:
            # length limiter for testing
            if len(URIS) > 1000:
                break
            g = crawl_page(p)
            p = get_next_page_uri(g)


def crawl_page(page_uri):
    g = get_graph_from_uri(page_uri)
    start_length = len(URIS)
    URIS.extend(get_contained_item_class_uris(g))
    end_length = len(URIS)

    print('Added {} URIs to the list from Page {}'.format(end_length - start_length, page_uri))
    return g


if __name__ == '__main__':
    crawl_register('http://localhost:5000/address/')

    print('Total URIs: {}'.format(len(URIS)))

    print(URIS[:10])


