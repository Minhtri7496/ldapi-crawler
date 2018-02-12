import os
import logging
import requests
import requests.exceptions
import rdflib
import argparse
import settings


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
        ORDER BY ?uri
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


# TODO: extend to crawling Register of Registers
def crawl_register(register_uri, reg_file):
    global uri_cache
    '''
    - get the register_uri RDF
    - load it into a graph
    - query out the URIs
    - load them into a list
    - if the list is >= URI_CACHE_MAX_LENGTH, write them to register file, empty cache
    - query out any next page
    - continue from 'load it into a graph'
    '''
    m = 'Crawling register {}'.format(register_uri)
    logging.info(m)
    print(m)

    # page through the register, storing results in register files
    page_uri = register_uri  # first page
    i = 0
    while True:
        # i += 1      # TESTING only go for 20 pages
        # if i > 20:  # TESTING only go for 20 pages
        #     break   # TESTING only go for 20 pages
        # get the page's URIs
        m = 'Crawling page {}'.format(page_uri)
        logging.info(m)
        print(m)
        g = get_graph_from_uri(page_uri)
        # add the URIs to the cache
        uri_cache.extend(get_contained_item_class_uris(g))
        # if the cache is greater or equal to the max allowed cache size, append it to the reg file then empty it
        if len(uri_cache) >= settings.URI_CACHE_MAX_LENGTH:
            with open(reg_file, 'a') as f:
                f.write('\n'.join(uri_cache))
                f.write('\n')
            uri_cache.clear()

        # check if there's another page, if not, break, we are done else iterate to next page
        next_page_uri = get_next_page_uri(g)
        if next_page_uri is None:
            break
        else:
            page_uri = next_page_uri


def crawl_instances_from_reg_file(reg_file, destination):
    '''
    - If destination is a local file ('local)
    - for each URI in reg_file
    - get its contents
    - write them to data file, as n-triples
    - continue until data_file >= DATA_FILE_SIZE_MAX
    - make new data file
    - repeat from 'for each URI in reg file

    - If destination is a SPARQL endpoint
    - Post results from each URI on reg_file to it

    - delete reg file
    '''
    # TODO: change these single-threaded loops to async
    last_uri_crawled = None
    try:  # write data to local files
        if not destination.startswith('http'):
            data_file_stem = 'data-'
            data_file_count = 0
            with open(reg_file, 'r') as f:
                # every DATA_FILE_LENGTH_MAX URIs, create a new file
                for idx, uri in enumerate(f.readlines()):
                    last_uri_crawled = uri.strip()
                    # every DATA_FILE_LENGTH_MAXth URI, create a new destination file
                    if (idx + 1) % settings.DATA_FILE_LENGTH_MAX == 0:
                        data_file_count += 1
                    with open(data_file_stem + str(data_file_count).zfill(4) + '.nt', 'a') as fl:
                        m = 'Saving RDF from {} to {}'.format(
                            last_uri_crawled,
                            data_file_stem + str(data_file_count).zfill(4) + '.nt'
                        )
                        logging.info(m)
                        print(m)
                        fl.write(get_graph_from_uri(last_uri_crawled).serialize(format='nt').decode('utf-8'))
        else:  # POST data to a SPARQL endpoint
            # make an HTTP session then, for each URI in URIs, get it's RDF and POST it
            s = requests.Session()
            if args.usr is not None and args.pwd is not None:
                s.auth = (args.usr, args.pwd)
            s.headers.update({'Accept': 'text/turtle'})
            with open(reg_file, 'r') as f:
                for idx, uri in enumerate(f.readlines()):
                    last_uri_crawled = uri.strip()
                    print('POSTing RDF from {} to {}'.format(last_uri_crawled, args.destination))
                    post_triples_to_sparql_endpoint(get_graph_from_uri(last_uri_crawled), args.destination, s)

        # this reg file has been crawled, so delete it
        os.unlink(reg_file)
        logging.info('Register file {} deleted as crawl complete'.format(reg_file))
    except Exception as e:
        logging.error(e)
    finally:
        # write last URI successfully retrieved to a file
        with open('last_uri_crawled.txt', 'w') as f:
            f.write(last_uri_crawled)


def post_triples_to_sparql_endpoint(g, post_uri, session):
    ntriples = g.serialize(format='ntriples')
    sparql_insert = 'INSERT DATA\n{{\n{}}}'.format(
        '\n'.join(['\t' + line for line in ntriples.decode('utf-8').splitlines()]))  # nice SPARQL formatting

    r = session.post(post_uri, data=sparql_insert, timeout=1)
    if 200 > r.status_code > 300:
        print(r.status_code)
        m = 'The INSERT was not successful. The SPARQL _database\' error message is: {}'.format(r.content)
        logging.info(str(m))
        raise requests.exceptions.ConnectionError(m)
    log_msg = 'INSERTed {} triples into triplestore'.format(len(g))
    logging.debug(log_msg)
    print(log_msg)


if __name__ == '__main__':
    # setup logging
    logging.basicConfig(filename='results.log', level=logging.INFO)  # logging.basicConfig(format='%(asctime)s %(message)s')

    # set up command line arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'uri',
        type=str,
        help='The crawler starting point URI.'
    )
    parser.add_argument(
        'destination',
        type=str,
        help='Where to store the RDF: if a string starting http:// is given, an attempt will be made to POST a '
             'SPARQL INSERT DATA command there. If another string, a local file will be created with that name '
             'and n-triples RDF saved into it.'
    )
    parser.add_argument(
        '-u',
        action="store",
        type=str,
        help='A username if a secured SPARQL endpoint is used',
        default=None
    )
    parser.add_argument(
        '-p',
        action="store",
        type=str,
        help='A password if a secured SPARQL endpoint is used',
        default=None
    )

    args = parser.parse_args()

    # start a crawling a register from a given register or register page URI
    reg_file = 'register.txt'
    uri_cache = []
    crawl_register(args.uri, reg_file)
    logging.info('All register URIs stored in file \'{}\''.format(reg_file))

    # crawl each instance URI stored in register cache files
    crawl_instances_from_reg_file(reg_file, args.destination)

    print('crawl completed')
