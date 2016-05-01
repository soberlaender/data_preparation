''' Creation of link structure'''

## TODO: change/check loading of files (esp. mapping)

# -*- coding: utf-8 -*-

from __future__ import division, print_function, unicode_literals

import collections
import cPickle as pickle
#import pickle
import gzip
import hashlib
import os
import pdb
import re
import sys
import time
import numpy as np
import urllib2
import io
import scipy.sparse as sp
import scipy


dump_date = "20160111"
start = "20160101-000000"
end = "20160215-000000"
domains = {
    "de.d": "wiktionary",
    "de.v": "wikiversity",
    "de.q": "wikiquote",
    "de.n": "wikinews",
    "de.b": "wikibooks",
    "fr.d": "wiktionary",
    "fr.v": "wikiversity",
    "fr.q": "wikiquote",
    "fr.n": "wikinews",
    "fr.b": "wikibooks"}

pageview_dir = os.path.join('data', 'pageviews')

# mapping of old id to new id
mapping = {}
id_per_title = {}
title_per_id = {}

# contains all links where one of the two pages is not found (for redirect purposes)
exceptions = {}

def read_pickle(fpath):
    with open(fpath, 'rb') as infile:
        obj = pickle.load(infile)
    return obj


def write_pickle(fpath, obj):
    with open(fpath, 'wb') as outfile:
        pickle.dump(obj, outfile, -1)


# only here for testing something (not relevant for this part)
def create_new_txt_files():
    txt_files = set([f for f in os.listdir(pageview_dir + "/sql_files") if f.endswith('.txt')])
    for filename in txt_files:
        if "pagelinks" in filename:
            print("unzip: ", filename)

            with gzip.open(os.path.join(pageview_dir + "/sql_files/", filename.replace(".txt", ".sql.gz")), 'rb') as infile:
                data = infile.readlines()
            with io.open(os.path.join(pageview_dir + "/sql_files", filename), 'w', encoding = 'utf-8') as outfile:
                for line in data:
                    line = line.decode('utf-8')

                    if (line.startswith("INSERT INTO")):
                        vals_tmp = (line[line.find("),("):-3])
                        vals_tmp = (vals_tmp.replace("),(", "\n"))
                        outfile.write(vals_tmp[1:])



# # create new mapping.p file from txt file (if new) and load id_per_title.p # #

def create_dict(new):

    if new:
        print("Write new mapping.p files")
        for domain in domains.keys():
            current_domain = domain[:2] + domains[domain]
            directory = pageview_dir + "/results/" + current_domain + "/"
            with io.open(os.path.join(directory, "mapping.txt"), 'r', encoding = 'utf-8') as mapfile:
                mappings = mapfile.readlines()
            mapping[domain] = {}

            for line in mappings:
                split_line = line.split(" ")
                mapping[domain][split_line[2][:-1]] = split_line[0]

            write_pickle(directory + "mapping.p", mapping[domain])

    print("Load id_per_title dict")
    id_per_title.update(read_pickle(pageview_dir + "/sql_files/id_per_title.p"))



# # read links from pagelinks file and create adjacency matrix from it (incl. redirects) # #

def read_links():

    txt_files = set([f for f in os.listdir(pageview_dir + "/sql_files") if f.endswith('.txt')])
    round_count = 0
    print("Process data")

    for domain in domains.keys():
        current_domain = domain[:2] + domains[domain]
        print(current_domain)
        print("     load mapping")
        current_mapping = read_pickle( pageview_dir + "/results/" + current_domain + "/" + "mapping.p")

        # create new adjacency matrix
        A = sp.csr_matrix((len(current_mapping), len(current_mapping))).tolil()

        print("     load pagelinks")
        with io.open(os.path.join(pageview_dir + "/sql_files", current_domain + "-" + dump_date + "-pagelinks.txt"), 'r', encoding = 'utf-8') as infile:
            while True:
                round_count += 1
                line = infile.readline()

                if not line:
                    break

                if (round_count % 100000) == 0:
                    print("Still working: ", line)

                split_line = line.split(",")

                if len(split_line) < 4:
                    #print("ERROR")
                    continue

                # only consider lines where both pages are within namespace 'article'
                if split_line[1] == '0' and split_line[-1] == '0\n':

                    help_count = 3
                    while not split_line[2].endswith('\''):
                        try:
                            split_line[2] += ',' + split_line[help_count]
                            help_count += 1
                        except:
                            split_line[2] += '\''
                    split_line[2] = domain + " " + split_line[2][1:-1]

                    try:
                        A[int(current_mapping[split_line[0]]), int(current_mapping[id_per_title[split_line[2]][5:]])] = 1

                    except:
                        try:
                            exceptions[id_per_title[split_line[2]]] = split_line[0]
                        except:
                            pass

                else:
                    continue


        print("     load redirects")
        with io.open(os.path.join(pageview_dir + "/sql_files", current_domain + "-" + dump_date + "-redirect.txt"), 'r', encoding = 'utf-8') as infile:
            redirects = infile.readlines()

            print("     Size of redirects: ", len(redirects))
            print("    ", A.getnnz(), " -- type: ", type(A))

            counter = 0

            # Loop redirect data
            print("     adding links from redirects")
            for line in redirects:
                split_line = line.split(",")
                if split_line[1] == '0':
                    help_count = 3
                    while not split_line[2].endswith('\''):
                        split_line[2] += ',' + split_line[help_count]
                        help_count += 1
                    split_line[2] = domain + " " + split_line[2][1:-1]


                    try:
                        A[int(current_mapping[exceptions[domain + " " + split_line[0]]]), int(current_mapping[id_per_title[split_line[2]][5:]])] = 1

                        counter += 1
                    except:
                        pass

            print("     # of found redirected pages: ", counter)
            print("    ", A.getnnz(), " -- type: ", type(A))

            exceptions.clear()

        directory = pageview_dir + "/results/" + current_domain + "/"
        if not os.path.exists(directory):
            os.makedirs(directory)
        if A.getnnz() > 0:
            print("     make A sparse")
            A = sp.csr_matrix(A)
            print("write A into pickle file with elements: ", A.getnnz())
            write_pickle(directory + "A.p", A)





if __name__ == '__main__':
    #create_new_txt_files()
    create_dict(False)
    read_links()
