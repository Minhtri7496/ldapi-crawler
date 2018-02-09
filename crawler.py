import sys
import logging
import requests
import requests.exceptions
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
    m = 'Crawling from {}'.format(register_uri)
    logging.info(m)
    print(m)
    # start by loading the register URIs
    g = get_graph_from_uri(register_uri)

    # get the containedItemClass URIs from this Register/Page, regardless of if there are more Pages or not
    URIS.extend(get_contained_item_class_uris(g))
    logging.info('Added {} URIs to the list from Page {}'.format(len(URIS), register_uri))

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
            g = crawl_register_page(p)
            p = get_next_page_uri(g)


def crawl_register_page(register_page_uri):
    g = get_graph_from_uri(register_page_uri)
    start_length = len(URIS)
    URIS.extend(get_contained_item_class_uris(g))
    end_length = len(URIS)

    m = 'Added {} URIs to the list from Page {}'.format(end_length - start_length, register_page_uri)
    logging.info(m)
    print(m)
    return g


def post_triples_to_triplestore(g, post_uri):
    ntriples = g.serialize(format='ntriples')
    sparql_insert = 'INSERT DATA\n{{\n{}}}'.format(
        '\n'.join(['\t' + line for line in ntriples.decode('utf-8').splitlines()]))  # nice SPARQL formatting

    # POST the SPARQL to the Fuseki endpoint
    auth = (None, None)
    headers = {'Accept': 'text/turtle'}
    try:
        r = requests.post(post_uri, headers=headers, data=sparql_insert, auth=auth, timeout=1)
        if 200 > r.status_code > 300:
            print(r.status_code)
            m = 'The INSERT was not successful. The SPARQL _database\' error message is: {}'.format(r.content)
            logging.info(str(m))
            raise requests.exceptions.ConnectionError(m)
        log_msg = 'INSERTed {} triples into triplestore'.format(len(g))
        logging.debug(log_msg)
        print(log_msg)
        return True
    except requests.ConnectionError as e:
        logging.info(str(e))
        exit(1)


if __name__ == '__main__':
    logging.basicConfig(filename='crawler.log', level=logging.INFO)
    crawl_register('http://localhost:5000/address/?per_page=10000&page=1')

    logging.info('Total URIs found in register: {}'.format(len(URIS)))

    # for each URI in URIs, get it's RDF
    for uri in URIS:
        print('Trying to insert from {}'.format(uri))
        post_triples_to_triplestore(get_graph_from_uri(uri), 'http://localhost:3030/gnaf/update')
