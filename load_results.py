#!/usr/bin/env python

import json
import sys
import os
from elasticsearch import Elasticsearch

def main():
    es = Elasticsearch(['http://localhost:9200'])

    results = find_json(sys.argv[1])

    # Let's loop this so we don't chew up all the RAM
    for one_json in results:

        fh = open(one_json, 'r')
        data = json.load(fh)

        print("Loading %s" % one_json)
        # Only load dictionaries
        if isinstance(data['results'], dict):
            for i in data['results'].keys():
                es_id = "%s-%s" % (data['results'][i]['rpm'], i)
                try:
                    es.update(id=es_id, index="ctl-results", doc_type='doc', body={'doc' :data['results'][i], 'doc_as_upsert': True})
                except:
                    pass

def find_json(path):

    find_results = []

    rootDir = path
    for dirName, subdirList, fileList in os.walk(path):
        for fname in fileList:
            if fname.endswith('.json'):
                the_file = os.path.join(dirName, fname)
                find_results.append(the_file)

    return find_results

if __name__ == "__main__":
    # execute only if run as a script
    main()
