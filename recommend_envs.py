from mimetypes import init
from secrets import choice
import requests
import urllib3
import sys
import json
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import os
import re
import time
import random
import logging
from pathlib import Path
from datetime import date
from sklearn.feature_extraction import DictVectorizer
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# testing
np.set_printoptions(threshold=sys.maxsize)

# global vars
NO_OF_REF_ENVS = 4
NO_OF_RAND_ENVS = 16
NO_OF_RAND_FFORMATS = 107
FILE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(FILE_PATH, 'sfdata2')


class Logger:
    '''Logger Class for data recording'''
    def __init__(self, level=logging.INFO) -> None:
        logpath = "./logs/" + date.today().strftime("%d_%m_%Y") + "/"
        filename = "debug"
        Path(logpath).mkdir(parents=True, exist_ok=True)
        logging.basicConfig(level=level,
                            format="%(asctime)s [%(levelname)-10s]"
                                   + "[%(module)-5s] [%(funcName)-10s] %(message)s",
                            handlers=[logging.FileHandler("{0}/{1}_{2}.log"
                                                          .format(logpath, filename,
                                                          time.asctime(time.localtime()))),
                                      logging.StreamHandler()
                                     ])
        self.logger = logging.getLogger('root')

    @property
    def log(self):
        return self.logger

def read_siegfried(siegfried_file_name):
    '''Reading siegfried file'''
    sg_file = open(siegfried_file_name)
    try:
        sg_data = json.load(sg_file)
        return sg_data
    except:
        logger.log.error("Error loading {}".format(siegfried_file_name))
        return None

def app_to_fformat_map():
    '''Mappings for application wiki
    tags to its supported file formats'''

    tags = ["Q18168774" ,"Q11261","Q29482", "Q18698690",
        "Q10393867", "Q134067", "Q862505", "Q11255",
        "Q11272", "Q80689", "Q60693055", "Q129793", "Q11215",
        "Q171477", "Q29482", "Q698", "Q171477", "Q11261",
        "Q11255", "Q11272", "Q11266","Q60691254","Q381",
        "Q698", "Q207902", "Q8041", "Q171477", "Q131382",
        "Q6930567", "Q862505", "Q10135", "Q3774510",
        "Q319417", "Q8038", "Q201809","Q17107792",
        "Q698", "Q9589", "Q70060004", "Q60693055",
        "Q862505", "Q11272", "Q11266", "Q129793"]

    readable_text_formats = False
    writable_text_formats = False
    app2fformats = {}

    logger.log.info("Application to file format mapping starting")
    for tag in tags:
        logger.log.debug("Extracting file formats data for app tag {}".format(tag))
        app2fformats[tag] = {}

        logger.log.debug("request GET for https://www.wikidata.org/wiki/{}".format(tag))
        wiki_res = requests.get("https://www.wikidata.org/wiki/" + tag)
        logger.log.debug("response for GET https://www.wikidata.org/wiki/{0} : {1}"
                         .format(tag, wiki_res.status_code))
        
        soup = BeautifulSoup(wiki_res.text, 'html.parser')
        read_file_formats = []
        write_file_formats = []
        for link in soup.findAll('a'):
            fformat_wikitag = None
            title_tag = link.get('title')
            if link.get('title') and re.search("(Q\d+)", link.get('title')):
                fformat_wikitag = re.search("(Q\d+)", link.get('title')).group(1)

            if (link.get('title') == "Property:P1072"):
                logger.log.debug("Readable File Formats")
                readable_text_formats = True
                writable_text_formats = False
            
            if link.get('title') == "Property:P1073":
                logger.log.debug("Writable File Formats")
                writable_text_formats = True
                readable_text_formats = False
                                
            if fformat_wikitag and readable_text_formats:
                logger.log.debug("readable_formats: {} {}".format(link.text, fformat_wikitag))
                read_file_formats.append(fformat_wikitag)
            if fformat_wikitag and writable_text_formats:
                logger.log.debug("writable_formats: {} {}".format(link.text, fformat_wikitag))
                write_file_formats.append(fformat_wikitag)
            if writable_text_formats and title_tag and \
                re.search("Property:\w\d+", link.get('title')) and \
                link.get('title') not in \
                    ["Property:P854", "Property:P813", "Property:P1476", "Property:P123", "Property:P1073"]: 
                writable_text_formats = False
            if readable_text_formats and title_tag and \
                re.search("Property:\w\d+", link.get('title')) and \
                link.get('title') not in \
                    ["Property:P854", "Property:P813", "Property:P1476", "Property:P123", "Property:P1072"]: 
                readable_text_formats = False

        # data sorting
        app2fformats[tag]["read_formats"] = [file_format_tag for file_format_tag in 
                        read_file_formats if read_file_formats.count(file_format_tag) == 1]
        app2fformats[tag]["write_formats"] = [file_format_tag for file_format_tag in
                        write_file_formats if write_file_formats.count(file_format_tag) == 1]

    try:
        with open("app2fformats.json", "w") as outfile:
            json.dump(app2fformats, outfile)
        logger.log.info("Applicaiton mapping sucessfully written to app2fformats.json")
    except:
        logger.log.error("Application mapping writing to app2fformats.json failed")

def load_emulation_envs(env_filename):
    '''Load Emulation environments to OpenSearch'''
    _debug = False
    headers = {'Content-Type': 'application/x-ndjson',}
    logger.log.info("Start emulation environments loading")
    logger.log.info("Reading file {}".format(env_filename))
    with open(env_filename, 'rb') as f:
        data = f.read()

    logger.log.info("request POST https://localhost:9200/_bulk")
    response = requests.post('https://localhost:9200/_bulk',
                             headers=headers, data=data,
                             verify=False, auth=('admin', 'admin'))

    logger.log.info("response for POST 'https://localhost:9200/_bulk' : {}"
                    .format(response.status_code))
    if response.status_code == 200:
        logger.log.info("environment data successfully loaded")

    if _debug:
        # index(=documents) and type(=document) only
        # matches with test environments(machines.json)
        resp = requests.get('https://localhost:9200/documents/document/1',
                            verify=False, auth=('admin', 'admin'))
        print(resp.json())

def test_query_envs():
    '''Test environment querying'''
    headers = {'Content-Type': 'application/x-ndjson',}
    json_data = {
        'query': {
            'query_string': {
                'query': 'word',
            },
        },
    }
    logger.log.info("Test simple environment querying")
    logger.log.info("request POST https://localhost:9200/documents/document/_search")
    response = requests.post('https://localhost:9200/documents/document/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
    logger.log.info("response for POST https://localhost:9200/documents/document/_search : {}"
                    .format(response.status_code))
    logger.log.info("response output {}".format(response.json()['hits']['hits']))

def delete_index(index):
    '''Delete Index in OpenSearch'''
    logger.log.info("Deleting Index {}".format(index))
    response = requests.delete('https://localhost:9200/'+index,verify=False, auth=('admin', 'admin'))
    print(response)

def query_match_envs(field, term):
    ''' Environment querying with match'''
    headers = {'Content-Type': 'application/x-ndjson',}
    json_data = {
        "query": {
            "match": {
                field: term
            }
        }
    }
    logger.log.info("environment querying for term {} in field {}".format(term, field))
    logger.log.info("request POST https://localhost:9200/environments/env/_search")

    # 'https://localhost:9200/environments/env/_search?explain=true
    response = requests.post('https://localhost:9200/environments/env/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
    logger.log.info("response for POST https://localhost:9200/environments/env/_search : {}"
                    .format(response.status_code))
    logger.log.info("response output {}".format(response.json()['hits']['hits']))
    return response.json()['hits']['hits']

def query_multi_match_envs(field, term):
    ''' Environment querying with multi match'''
    headers = {'Content-Type': 'application/x-ndjson',}
    json_data = {
        "query": {
            "multi_match": {
                "query": term,
                "fields": field
            }
        }
    }
    logger.log.info("environment querying for term {} in field {}".format(term, field))
    logger.log.info("request POST https://localhost:9200/environments/env/_search")

    # 'https://localhost:9200/environments/env/_search?explain=true
    response = requests.post('https://localhost:9200/environments/env/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
    logger.log.info("response for POST https://localhost:9200/environments/env/_search : {}"
                    .format(response.status_code))
    logger.log.info("response output {}".format(response.json()['hits']['hits']))
    return response.json()['hits']['hits']

def test_query_multi_envs():
    ''' Test multiple environment querying'''
    headers = {'Content-Type': 'application/x-ndjson',}
    json_data = {
        "query": {
            "multi_match": {
                "query": "word+vim",
                "fields": ["software^4", "machine"]
            }
        }
    }
    logger.log.info("Test multiple environment querying")
    logger.log.info("request POST https://localhost:9200/documents/document/_search")
    response = requests.post('https://localhost:9200/documents/document/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
    logger.log.info("response for POST https://localhost:9200/documents/document/_search : {}"
                    .format(response.status_code))
    logger.log.info("response output {}".format(response.json()['hits']['hits']))


def test_fileformat_to_app_query_envs():
    '''Test fileformat to application querying'''
    headers = {'Content-Type': 'application/x-ndjson',}
    fformat_query = '.dll+.exe+txt'
    json_data = {
        'query': {
            'query_string': {
                'query': fformat_query,
            },
        },
    }
    logger.log.info("Test Fileformat to Application querying")
    logger.log.info("query: {}".format(fformat_query))
    logger.log.info("request POST https://localhost:9200/documents1/document1/_search")
    response = requests.post('https://localhost:9200/documents1/document1/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
    logger.log.info("response for POST https://localhost:9200/documents1/document1/_search : {}"
                    .format(response.status_code))
    logger.log.info("response output {}".format(response.json()['hits']['hits']))
    app = response.json()['hits']['hits'][0]['_source']['app']
    if app:
        logger.log.info("Application found {}".format(app))
        json_data = {
            'query': {
                'query_string': {
                    'query': app,
                },
            },
        }
        logger.log.info("qureying for application {}".format(app))
        logger.log.info("request POST https://localhost:9200/documents/document/_search")
        response = requests.post('https://localhost:9200/documents/document/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
        logger.log.info("response for POST https://localhost:9200/documents/document/_search : {}"
                    .format(response.status_code))
        logger.log.info("response output {}".format(response.json()['hits']['hits']))
    else:
        logger.log.info("No suitable application found")


def fformat_to_app_map():
    # Q86920 (.txt), Q218170 (.ps) 
    url = "https://www.wikidata.org/entity/Q86920"
    try:
        fileformat_key = re.search("/entity/(\w\d+)", url).group(1)
    except:
        print("Fileformat key extract error.")

    # wikidata what links here
    response = requests.get("https://www.wikidata.org/wiki/Special:WhatLinksHere/" + fileformat_key)
    print(response.status_code)

    soup = BeautifulSoup(response.text, 'html.parser')
    # collecting apps which can read the file format. 
    apps = []
    for link in soup.findAll('a'):
        if link.get('href'):
            wikitag_extract = re.search("/wiki/(\w\d+)", link.get('href'))
            if wikitag_extract and wikitag_extract.group(1) not in [fileformat_key]:
                app_wikitag = wikitag_extract.group(1)
                app_name = (re.sub(r"(\u200e)", "", link.get('title')))
                apps.append((app_wikitag, app_name))

    readable_apps = []
    writable_apps = []
    # finding matching application with the readable file format.
    for app, app_name in apps:
        wiki_res = requests.get("https://www.wikidata.org/wiki/" + app)
        soup = BeautifulSoup(wiki_res.text, 'html.parser')

        readable_text_formats = False
        writable_text_formats = False
        for link in soup.findAll('a'):
            title_tag = link.get('title')
            fformat_wikitag = None
            if link.get('title') and re.search("(Q\d+)", link.get('title')):
                fformat_wikitag = re.search("(Q\d+)", link.get('title')).group(1)

            if fformat_wikitag and readable_text_formats:
                print("readable_formats: ", link.text)
                if fformat_wikitag == fileformat_key:
                    readable_apps.append((app, app_name))

            if fformat_wikitag and writable_text_formats:
                print("writable_formats: ", link.text)
                if fformat_wikitag == fileformat_key:
                    writable_apps.append((app, app_name))

            if (link.get('title') == "Property:P1072"):
                print("readable_file_formats")
                readable_text_formats = True
                writable_text_formats = False

            if link.get('title') == "Property:P1073":
                print("writable_file_formats")
                writable_text_formats = True
                readable_text_formats = False

            if writable_text_formats and title_tag and \
                re.search("Property:\w\d+", link.get('title')) and \
                link.get('title') not in \
                    ["Property:P854", "Property:P813", "Property:P1073"]: 
                writable_text_formats = False

            if readable_text_formats and title_tag and \
                re.search("Property:\w\d+", link.get('title')) and \
                link.get('title') not in \
                    ["Property:P854", "Property:P813", "Property:P1072"]: 
                readable_text_formats = False
    print("read apps:\n", readable_apps)
    print("\nwrite apps:\n", writable_apps)

def write_app2fformats(envs, write_envs_filename):
    '''
        Writing {reference/random}_envs.json files
        NOTE: newline at the bottom is required.

        steps:
        1. Reading Applications to File formats Map.
        2. For each of the environment in 'envs'
           collect all the relevant file formats.
        3. Write to a json.
    '''

    # reading app to file formats map
    # 'app2fformats.json' is created by app_to_fformat_map()
    app2ff_filename = 'app2fformats.json'
    if not os.path.exists(app2ff_filename):
        logger.log.error("{} does not exists".format(app2ff_filename))
    app2ff_file = open(app2ff_filename)
    app2ff_map = json.load(app2ff_file)

    # environment json output
    if os.path.exists(write_envs_filename):
        os.remove(write_envs_filename)
    envs_file = open(write_envs_filename, 'a')

    file_formats = []
    for id, (env, apps) in enumerate(envs.items()):
        fformats = []
        env2ff_map = {}
        for app in apps:
            fformats.append(app2ff_map[app]['read_formats'])
        
        # creating flat list
        flat_fformats = []
        [flat_fformats.append(f) for ff in fformats for f in ff]
        # remove duplicates
        unique_fformats = []
        [unique_fformats.append(f) for f in flat_fformats if f not in unique_fformats]

        # setup index for opensearch
        if write_envs_filename == 'random_envs.json':
            env_index = {"index": {"_index":"environments",
                                    "_type": "env",
                                    "_id": id+1+NO_OF_REF_ENVS}}
        else:
            env_index = {"index": {"_index":"environments",
                                    "_type": "env",
                                    "_id": id+1}}
        json.dump(env_index, envs_file)
        envs_file.write("\n")

        # setup env2ff_map
        env2ff_map["machine"] = env
        env2ff_map["fformats"] = unique_fformats
        json.dump(env2ff_map, envs_file)
        envs_file.write("\n")

        file_formats.append(unique_fformats)

    envs_file.close()
    logger.log.info("Emulation environments mapping " +
                    "sucessfully written to {}".format(write_envs_filename))
    return file_formats

def create_refrence_envs():
    '''Create reference environments.
       These tags are fixed'''
    ref_envs = {
        "ref_env_1" : ["Q18168774" ,"Q11261","Q29482", "Q18698690",
                   "Q10393867", "Q134067", "Q862505", "Q11255",
                   "Q11272", "Q80689", "Q60693055", "Q129793"],
        "ref_env_2" : ["Q11215" , "Q171477" , "Q29482", "Q698", "Q171477",
                   "Q11261", "Q11255", "Q11272", "Q11266","Q60691254"],
        "ref_env_3" : ["Q381" , "Q698", "Q207902", "Q8041", "Q171477",
                   "Q131382", "Q6930567", "Q862505", "Q10135",
                   "Q3774510", "Q319417", "Q8038", "Q201809"],
        "ref_env_4" : ["Q17107792", "Q698", "Q9589",
                   "Q70060004", "Q60693055", "Q862505",
                   "Q11272", "Q11266", "Q129793"]}
    ref_envs_list = write_app2fformats(ref_envs, 'reference_envs.json')
    load_emulation_envs('reference_envs.json')
    return ref_envs_list

def random_envs():
    '''Create random emulation environments
       Using fixed reference environments'''
    # reading app to file formats map
    # 'app2fformats.json' is created by app_to_fformat_map()
    app2ff_filename = 'app2fformats.json'
    if not os.path.exists(app2ff_filename):
        logger.log.error("{} does not exists".format(app2ff_filename))
    app2ff_file = open(app2ff_filename)
    app2ff_map = json.load(app2ff_file)

    # keys into a list
    app_keys = list(app2ff_map.keys())
    rand_envs = {}
    for id in range(NO_OF_RAND_ENVS):
        items = np.random.choice(app_keys, size=NO_OF_RAND_FFORMATS, replace=False)
        env_name = "rand_env_" + str(id+1+NO_OF_REF_ENVS)
        rand_envs[env_name] = list(items)
    write_app2fformats(rand_envs, 'random_envs.json')

def create_random_envs_index(random_envs, write_envs_filename):
    '''
       This function creates random envs indexes using file
       format data collected directly from the image(sdata2) files.
    '''
    # environment json output
    if os.path.exists(write_envs_filename):
        os.remove(write_envs_filename)
    envs_file = open(write_envs_filename, 'a')

    for id in range(NO_OF_RAND_ENVS):
        env_name = "rand_env_" + str(id+1+NO_OF_REF_ENVS)
        env_index = {"index": {"_index":"environments",
                               "_type": "env",
                               "_id": id+1+NO_OF_REF_ENVS}}
        json.dump(env_index, envs_file)
        envs_file.write("\n")

        env2ff_map = {}
        # setup env2ff_map
        env2ff_map["machine"] = env_name
        env2ff_map["fformats"] = random_envs[id]
        json.dump(env2ff_map, envs_file)
        envs_file.write("\n")

    envs_file.close()
    logger.log.info("Emulation environments mapping " +
                    "sucessfully written to {}".format(write_envs_filename))

def create_knn_envs_index(knn_vectors, write_envs_filename):
    '''This function creates knn indexes using knn vectors
       index file format:
            { "index": { "_index": "knn-index", "_id": "1" } }
            { "fformats_vector": [1, 0, 0, 1], "machine": ref_env_0 }
    '''
    # environment json output
    if os.path.exists(write_envs_filename):
        os.remove(write_envs_filename)
    envs_file = open(write_envs_filename, 'a')

    # ref env knn-index
    for id in range(NO_OF_REF_ENVS):
        env_name = "ref_env_" + str(id+1)
        env_index = {"index": {"_index":"knn-index",
                               "_id": id+1}}
        json.dump(env_index, envs_file)
        envs_file.write("\n")

        env2ff_map = {}
        # setup env2ff_map
        env2ff_map["machine"] = env_name
        env2ff_map["fformats_vector"] = knn_vectors[id].tolist()
        json.dump(env2ff_map, envs_file)
        envs_file.write("\n")

    for id in range(NO_OF_RAND_ENVS):
        env_name = "rand_env_" + str(id+1+NO_OF_REF_ENVS)
        env_index = {"index": {"_index":"knn-index",
                               "_id": id+1+NO_OF_REF_ENVS}}
        json.dump(env_index, envs_file)
        envs_file.write("\n")

        env2ff_map = {}
        # setup env2ff_map
        env2ff_map["machine"] = env_name
        env2ff_map["fformats_vector"] = knn_vectors[id+NO_OF_REF_ENVS].tolist()
        json.dump(env2ff_map, envs_file)
        envs_file.write("\n")

    envs_file.close()
    logger.log.info("Emulation environments mapping " +
                    "sucessfully written to {}".format(write_envs_filename))
    return len(knn_vectors[id].tolist())

def random_envs_choice(fileformat_list):
    '''choose environments randomly'''
    choices = []
    for id in range(NO_OF_RAND_ENVS):
        items = np.random.choice(fileformat_list,
                                 size=NO_OF_RAND_FFORMATS, replace=False)
        choices.append(items.tolist())
    return choices


class Vectorizer():
    def __init__(self) -> None:
        self.vectorizer = DictVectorizer(sparse=False)
        self.vectors = []

    def fit_transform(self, x):
        return self.vectorizer.fit_transform(x)

    def transform(self, x):
        return self.vectorizer.transform(x)

    def list2dict(self, list):
        return {l : 1 for l in list}

    def create_vectors(self, fileformat_list):
        '''mapping for random environment lists
        to one-hot-coded vectors'''
        for list in fileformat_list:
            self.vectors.append(self.list2dict(list))
        # encoding vectors
        return self.fit_transform(self.vectors)

    def create_query_vectors(self, fileformat_list):
        qvectors = []
        for list in fileformat_list:
            qvectors.append(self.list2dict(list))
        return self.transform(qvectors)


def setup_perf_matrix():
    '''Setup of a performance matrix for each emulation
    environment'''
    perf_mat = {}
    perf_mat['Total'] = 0
    for id in range(NO_OF_REF_ENVS):
        perf_mat["ref_env_"+str(id+1)] = 0
    for id in range(NO_OF_RAND_ENVS):
        perf_mat["rand_env_"+str(id+1+NO_OF_REF_ENVS)] = 0
    return perf_mat

def calculate_score(matches, perf_mat):
    '''Calculate accumulated BM25 score values for each
        machine'''
    # two ways of performance calculations are implemented
    # if "machine_choice" is set True; then it will
    # do the ranking based on the best emulation environment
    # recommended by search results.
    # Else it will accumulate all recommended emu. envs.
    machine_choice = True
    if machine_choice:
        if matches:
            perf_mat[matches[0]['_source']['machine']] += 1
            perf_mat['Total'] += 1
    else:
        for match in matches:
            perf_mat[match['_source']['machine']] += match['_score']
    return perf_mat

def dump2json(data, filename):
    try:
        with open(filename, "w") as outfile:
            json.dump(data, outfile)
        logger.log.info("Sucessfully written to {}"
                        .format(filename))
    except:
        logger.log.error("Writing " +
                          "to {} failed".format(filename))

def get_matches(sg_data):
    '''Get best match wiki tag for each file.'''
    tags = []
    for file in sg_data['files']:
        match = file['matches'][0]['id']
        if match != "UNKNOWN":
            tags.append(match)
    tags = list(set(tags))
    return tags

def multiple_query_all_match(image_filename, perf_mat):
    '''query all matches wiki fileformat for each file
    i.e. each file format in iso'''
    sg_data = read_siegfried(image_filename)
    all_matches = []
    if sg_data:
        for file in sg_data['files']:
            for match in file['matches']:
                if match != "UNKNOWN":
                    all_matches.append(match['id'])
                    fformat_matches = query_match_envs("fformats", match['id'])
                    logger.log.info("Query response: {}".format(fformat_matches))
                    perf_mat = calculate_score(fformat_matches, perf_mat)
    return perf_mat

def single_query_all_match(image_filename, perf_mat):
    '''query all matching wiki fileformat for entire image file(iso)'''
    sg_data = read_siegfried(image_filename)
    all_matches = []
    if sg_data:
        for file in sg_data['files']:
            for match in file['matches']:
                if match['id'] != "UNKNOWN":
                    all_matches.append(match['id'])
        fformat_matches = query_multi_match_envs("fformats", '+'.join(set(all_matches)))
        logger.log.info("Query response: {}".format(fformat_matches))
        perf_mat = calculate_score(fformat_matches, perf_mat)
        logger.log.info("Query list: {}".format(set(all_matches)))
    return perf_mat, all_matches

def single_query_best_match(image_filename, perf_mat):
    '''query best match wiki fileformats for entire image file(iso)'''
    sg_data = read_siegfried(image_filename)
    all_matches = []
    if sg_data:
        for file in sg_data['files']:
            match = file['matches'][0]['id']
            if match != "UNKNOWN":
                all_matches.append(match)
        fformat_matches = query_multi_match_envs("fformats", '+'.join(all_matches))
        logger.log.info("Query response: {}".format(fformat_matches))
        perf_mat = calculate_score(fformat_matches, perf_mat)
        logger.log.info("Query list: {}".format(set(all_matches)))
    return perf_mat, all_matches

def single_query_knn(vectorizer, fileformat_list, perf_mat):
    vector = vectorizer.create_query_vectors([fileformat_list])
    query_vector = vector[-1].tolist()
    headers = {'Content-Type': 'application/json',}
    json_data = {
        'size': 4,
        'query': {
            'knn': {
                'fformats_vector': {
                    "vector": query_vector,
                    'k': 4
                }
            }
        }
    }
    json_data = {
        "size": 4,
        "query": {
            "script_score": {
                    "query": {
                        "match_all": {}
                    },
                    "script": {
                        "source": "knn_score",
                        "lang": "knn",
                        "params": {
                            "field": "fformats_vector",
                            "query_value": query_vector,
                            "space_type": "cosinesimil"
                        }
                    }
            }
        }
    }
    logger.log.info("knn querying")
    logger.log.info("request POST https://localhost:9200/my-index/_search")
    fformat_matches = requests.post('https://localhost:9200/knn-index/_search',
                            headers=headers, json=json_data,
                            verify=False, auth=('admin', 'admin'))
    logger.log.info("Query response: {}".format(fformat_matches))
    perf_mat = calculate_score(fformat_matches.json()['hits']['hits'], perf_mat)
    return perf_mat

def extract_fileformats():
    '''Extract all unique file formats by
    reading through all the siegfried input files.
    '''
    matches = []
    # collect wiki fileformats tags from
    # all siegfried files
    for (root, dirs, files) in os.walk(DATA_PATH):
        for f in files:
            if '.json' in f:
                image = read_siegfried(os.path.join(DATA_PATH, f))
                if image:
                    matches.extend(get_matches(image))
    # get count of all matches
    counts = pd.Series(matches).value_counts()
    # get unique tags only
    unique_matches = np.unique(np.array(matches))
    umatches_dict = {}
    # map into a dict.
    # tag: no of occurances
    for uid in unique_matches:
        umatches_dict[uid] = int(counts.get(uid))
    # sorting in descending
    sorted_umatches_set = sorted(umatches_dict.items(),
                            key=lambda item: item[1], reverse=True)
    umatches_dict = {v: k for v, k in sorted_umatches_set}
    dump2json(umatches_dict, "uniqueid_counts.json")
    return unique_matches

def create_random_envs(unique_fileformats):
    # choose random envs
    random_envs = random_envs_choice(unique_fileformats)
    # create random envs index
    create_random_envs_index(random_envs, 'random_envs.json')
    # load envs
    load_emulation_envs('random_envs.json')
    return random_envs

def create_knn_indexes(vectorizer, environment_file):
    '''create knn index with both random and reference data'''
    file = open(environment_file)
    env_data = json.load(file)
    ref_fileformats = env_data['ref_envs']
    rand_fileformats = env_data['rand_envs']
    # vetorization.
    encoded_vecs = vectorizer.create_vectors(ref_fileformats + rand_fileformats)
    dimension = create_knn_envs_index(encoded_vecs, 'knn_envs.json')
    create_knn_index_setup(dimension, 'knn_envs_setup.json')

def create_knn_index_setup(dimension, knn_setup_filename):
    '''Write setup file for knn indexing'''
    data = {
                "settings": {
                    "index.knn": False
                },
                "mappings": {
                    "properties": {
                        "fformats_vector": {
                            "type": "knn_vector",
                            "dimension": dimension
                        }
                    }
                }
            }
    dump2json(data, knn_setup_filename)

def store_knn_envs(ref_envs, rand_envs, environment_jsonfile):
    '''write environments in json file'''
    envs = {}
    envs["ref_envs"] = ref_envs
    envs["rand_envs"] = rand_envs
    dump2json(envs, environment_jsonfile)


def load_knn_indexes(knn_setup_filename, index_filename):
    '''Loading KNN-indexs in OpenSearch Server'''
    headers = {'Content-Type': 'application/json',}
    logger.log.info("request PUT https://localhost:9200/knn-index")
    response = requests.put('https://localhost:9200/knn-index',
                             headers=headers, data=open(knn_setup_filename),
                             verify=False, auth=('admin', 'admin'))

    env_filename = index_filename
    logger.log.info("Start knn indexes loading")
    logger.log.info("Reading file {}".format(env_filename))
    with open(env_filename, 'rb') as f:
        data = f.read()
    headers = {'Content-Type': 'application/x-ndjson',}
    logger.log.info("request POST https://localhost:9200/_bulk")
    response = requests.post('https://localhost:9200/_bulk',
                             headers=headers, data=data,
                             verify=False, auth=('admin', 'admin'))
    logger.log.info("response for POST https://localhost:9200/_bulk : {}"
                    .format(response.status_code))
    logger.log.info("response output {}".format(response.json()))

def log_summary(perf_mat, summary_filename):
    '''Logging Summary of the results'''
    filelevel_data = {}
    filelevel_data["Summary"] = sorted(perf_mat.items(),
                                       key=lambda item: item[1], reverse=True)
    dump2json(filelevel_data, summary_filename)


# global logger
logger = Logger()
if __name__ == "__main__":

    # vectorizer for knn mapping.
    vectorizer = Vectorizer()
    setup = True

    # performance score matrix
    perf_mat = setup_perf_matrix()
    perf_mat_knn = setup_perf_matrix()

    runs = 1
    for run in range(runs):
        logger.log.info("Run: {}".format(runs))
        if setup:
            # create reference environments.
            ref_envs = create_refrence_envs()
            # extract file formats randomly.
            fileformats = extract_fileformats()
            # create random environments.
            rand_envs = create_random_envs(fileformats)
            # create knn indexes.
            delete_index('knn-index')
            store_knn_envs(ref_envs, rand_envs, 'envs_for_knn.json')
            create_knn_indexes(vectorizer, 'envs_for_knn.json')
            load_knn_indexes('knn_envs_setup.json', 'knn_envs.json')
        else:
            load_emulation_envs('reference_envs.json')
            load_emulation_envs('random_envs.json')
            create_knn_indexes(vectorizer, 'envs_for_knn.json')
            load_knn_indexes('knn_envs_setup.json', 'knn_envs.json')

        # search querying..
        filecount = 0
        for (root, dirs, files) in os.walk(DATA_PATH):
            for file in files:
                if '.json' in file:
                    image_filename = os.path.join(DATA_PATH, file)
                    filecount += 1
                    perf_mat, fileformats = single_query_all_match(image_filename, perf_mat)
                    perf_mat_knn = single_query_knn(vectorizer, fileformats, perf_mat_knn)

    log_summary(perf_mat, 'summary.json')
    log_summary(perf_mat_knn, 'summary_knn.json')
    logger.log.info("file count {}".format(filecount))

    ref_env_count = 0
    rand_env_count = 0
    total = 0
    for env, count in perf_mat_knn.items():
        if "ref" in env:
            ref_env_count += perf_mat_knn[env]
        elif "rand" in env:
            rand_env_count += perf_mat_knn[env]
        elif "Total" in env:
            total += perf_mat_knn[env]
        else:
            logger.log.error("Undefined key {} in performance matrix".format(env))
    logger.log.info("Total Ref. Envs {}".format(ref_env_count))
    logger.log.info("Total Rand. Envs {}".format(rand_env_count))
    logger.log.info("Total Envs {}".format(total))
