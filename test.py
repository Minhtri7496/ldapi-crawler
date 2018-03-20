import rdflib

g = rdflib.Graph().parse('data-0003.nt', format='nt')
q = '''
    SELECT (COUNT(?s) AS ?cnt)
    WHERE {
        ?s a <http://gnafld.org/def/gnaf#Address>
    }
'''
for r in g.query(q):
    print(r)
