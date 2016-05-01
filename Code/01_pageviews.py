'''Counting of pageviews for a certain period of time'''


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
from datetime import date
import datetime
import io

# date of creation of dumps
dump_date = "20160111"

# start and end time in format: YYYYMMDD-HH0000
start = "20160101-000000"
end = "20160331-230000"

# contains domains which are counted (key = shortcut, value = name of wiki project)
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

# directory where everything should be saved
pageview_dir = os.path.join('data', 'pageviews')

# hashes
d = {}

# list of names of dump files with view counts
filenames = []

# list of names of other dump files (page.sql, pagelink.sql, redirect.sql)
other_files = []

# key = shortcut of domain + title of page, value = shortcut of domain + id of page
id_per_title = {}

# key = shortcut of domain + title of page, value = # views
views_per_title = {}


def read_pickle(fpath):
    with open(fpath, 'rb') as infile:
        obj = pickle.load(infile)
    return obj


def write_pickle(fpath, obj):
    with open(fpath, 'wb') as outfile:
        pickle.dump(obj, outfile, -1)


# # downloading of all files (param perform: flag if download/check should be performed) # #

def download(perform):

    download_hashes()
    base_url = 'http://dumps.wikimedia.org/other/pagecounts-raw/'
    if not os.path.exists(pageview_dir):
            os.makedirs(pageview_dir)
    files_present = set([f for f in os.listdir(pageview_dir) if f.endswith('.gz')])

    # counters for downloading all pagecount files from start to end date
    date_count = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    date_end = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
    days = (date_end - date_count).days
    hours = int(end[9:11]) - int(start[9:11])
    #print("We have ", days,  " days and ", hours, " hours to download")

    hour_count = int(start[9:11])
    hour_end = int(end[9:11])

    for day_count in range(0, days + 1):
        print("Start of day ", day_count, " of ", days, " days!")

        if (days - day_count) != 0:
            hour_end = 24;
        else:
            hour_end = int(end[9:11]) + 1

        for counter in range(hour_count, hour_end):

            filename = str(date_count)
            filename = 'pagecounts-' + filename.replace("-", "") + '-'

            if counter < 10:
                filename += '0' + str(counter) + '0000' + '.gz'
            else:
                filename += str(counter) + '0000' + '.gz'

            print(filename)

            if perform:
                if filename in files_present:
                    print('    already downloaded')
                    check_hash(filename)
                    filenames.append(filename)
                    continue

                final_url = base_url + str(date_count)[:4] + '/' + str(date_count)[:4] + '-' + str(date_count)[5:7]
                final_url += '/' + filename

                attempts = 0
                while attempts < 3:
                    try:
                        response = urllib2.urlopen(final_url, timeout=10)
                        content = response.read()
                        with open(os.path.join(pageview_dir, filename), 'w') as outfile:
                            outfile.write(content)
                        break
                    except urllib2.URLError as e:
                        attempts += 1
                        print(type(e))
                        pdb.set_trace()

                check_hash(filename)
                filenames.append(filename)

            else:
                filenames.append(filename)

        date_count += datetime.timedelta(days = 1)
        hour_count = 0



# # perform hash check for dump files # #

def check_hash(filename):

    if filename.endswith(".sql.gz"):
        directory = pageview_dir + "/sql_files"
    else:
        directory = pageview_dir

    f = open(os.path.join(directory, filename), 'rb')
    md5 = hashlib.md5()
    while True:
        data = f.read(8192)
        if not data:
            break
        md5.update(data)
    m = md5.hexdigest()

    if d[filename] != m:
        print(filename, " ", m, "MD5 HASH NOT OK")
    else:
        print(filename, "OK")




# # downloading of hash files for pagecounts dumps # #

def download_hashes():
    if not os.path.exists(pageview_dir + "/hash_files"):
            os.makedirs(pageview_dir + "/hash_files")

    hash_files = set([f for f in os.listdir(pageview_dir + "/hash_files") if f.endswith('.txt')])

    year = int(start[:4])
    year_count = int(end[:4]) - int(start[:4])

    for counter in range(0, year_count + 1):
        print(year)
        url = "http://dumps.wikimedia.org/other/pagecounts-raw/" + str(year) + '/'
        url += str(year) + '-' + str(start[4:6]) + "/md5sums.txt"
        print(url)

        if ("md5sums_" + str(year) + ".txt") in hash_files:
            print('    already downloaded')
            for line in open(os.path.join(pageview_dir + "/hash_files", "md5sums_" + str(year) + ".txt")):
                d[line.split()[1]] = line.split()[0]
            year += 1
            continue
        attempts = 0
        while attempts < 3:
            try:
                response = urllib2.urlopen(url, timeout=5)
                content = response.read()
                with open(os.path.join(pageview_dir + "/hash_files", "md5sums_" + str(year) + ".txt"), 'w') as outfile:
                    outfile.write(content)
                break
            except urllib2.URLError as e:
                attempts += 1
                print(type(e))
                pdb.set_trace()
        for line in open(os.path.join(pageview_dir + "/hash_files", "md5sums_" + str(year) + ".txt")):
            d[line.split()[1]] = line.split()[0]
        year += 1



# # downloading other dump files (page, pagelinks, redirect and their hash files) # #

def download_other_files(perform):
    if not os.path.exists(pageview_dir + "/sql_files"):
            os.makedirs(pageview_dir + "/sql_files")
    sql_files = set([f for f in os.listdir(pageview_dir + "/sql_files") if f.endswith('.gz')])
    hash_files = set([f for f in os.listdir(pageview_dir + "/hash_files") if f.endswith('.txt')])

    for domain in domains.keys():
        other_files.append(domain[:2] + domains[domain] + "-" + dump_date + "-page.sql.gz")
        other_files.append(domain[:2] + domains[domain] + "-" + dump_date + "-pagelinks.sql.gz")
        other_files.append(domain[:2] + domains[domain] + "-" + dump_date + "-redirect.sql.gz")
        other_files.append(domain[:2] + domains[domain] + "-" + dump_date + "-md5sums.txt")

    domain_counter = 0
    domain_keys = domains.keys()
    if perform:
        for filename in other_files:
            url = "http://dumps.wikimedia.org/" + domain_keys[domain_counter % 4][:2]
            url += domains[domain_keys[domain_counter % 4]] + "/" + dump_date + "/" + filename
            print(filename)

            if filename in sql_files or filename in hash_files:
                print('    already downloaded')
                continue

            attempts = 0
            while attempts < 5:
                try:
                    response = urllib2.urlopen(url, timeout=1)
                    content = response.read()
                    if filename.endswith(".sql.gz"):
                        with open(os.path.join(pageview_dir + "/sql_files", filename), 'w') as outfile:
                            outfile.write(content)
                    else:
                        with open(os.path.join(pageview_dir + "/hash_files", filename), 'w') as outfile:
                            outfile.write(content)
                    break
                except urllib2.URLError as e:
                    attempts += 1
                    #print("This it attempt #", attempts)
                    print(type(e))
                    pdb.set_trace()
            domain_counter += 1

        for line in open(os.path.join(pageview_dir + "/hash_files", other_files[3])):
            d[line.split()[1]] = line.split()[0]
        for filename in other_files:
            if filename.endswith(".sql.gz"):
                check_hash(filename)
            else:
                other_files.remove(filename)



# # parsing sql files into txt files # #

def make_txt_files():

    txt_files = set([f for f in os.listdir(pageview_dir + "/sql_files") if f.endswith('.txt')])

    for filename in other_files:
        print("unzip: ", filename)
        if filename.replace(".sql.gz", ".txt") in txt_files:
            print('    already created')
            continue

        if filename.endswith(".gz"):
            with gzip.open(os.path.join(pageview_dir + "/sql_files/", filename), 'rb') as infile:
                data = infile.readlines()
            with io.open(os.path.join(pageview_dir + "/sql_files", filename.replace(".sql.gz", ".txt")), 'w', encoding = 'utf-8') as outfile:

                for line in data:
                    line = line.decode('utf-8')

                    if (line.startswith("INSERT INTO")):
                        vals_tmp = (line[line.find("),("):-3])
                        vals_tmp = (vals_tmp.replace("),(", "\n"))
                        outfile.write(vals_tmp)



# # creating/loading dicts id_per_title and views_per_title # #

def create_dicts():
    data = []
    help_count = 0
    dict_count = 0

    txt_files = set([f for f in os.listdir(pageview_dir + "/sql_files") if f.endswith('.txt')])
    pickle_files = set([f for f in os.listdir(pageview_dir + "/sql_files") if f.endswith('.p')])

    # if dicts already as pickle files present, load them
    if "id_per_title.p" in pickle_files and "views_per_title.p" in pickle_files:
        print("Loading dicts from pickle files")
        id_per_title.update(read_pickle(pageview_dir + "/sql_files/id_per_title.p"))
        views_per_title.update(read_pickle(pageview_dir + "/sql_files/views_per_title.p"))
        print(len(id_per_title), " -- ", len(views_per_title))

    # else create them
    else:
        print("Creating dicts")
        for filename in other_files:
            if "page.sql" in filename:
                print(filename)

                with io.open(os.path.join(pageview_dir + "/sql_files/", filename.replace(".sql.gz", ".txt")), 'r', encoding = 'utf-8') as infile:
                    data = infile.readlines()

                for line in data:
                    split_line = line.split(',')

                    # only take pages within namespace 'article'
                    if split_line[1] == '0':
                        help_count = 3

                        # help routine for fixing errors if '\'' within page name
                        while not split_line[2].endswith('\''):
                            split_line[2] += ',' + split_line[help_count]
                            help_count += 1

                        name = domains.keys()[dict_count] + ' ' + split_line[2][1:-1]
                        id_per_title[name] = domains.keys()[dict_count] + ' ' + split_line[0]

                        # only to count if it is not a redirect page
                        if split_line[5] == '0':
                            views_per_title[name] = 0

                dict_count += 1
        write_pickle(pageview_dir + "/sql_files/id_per_title.p", id_per_title)
        write_pickle(pageview_dir + "/sql_files/views_per_title.p", views_per_title)




# # finding for domains relevant lines within pagecounts files # #

def find_domain_entries():

    data_to_write = []
    data_ = []

    # flag if something needs to be written in an outfile
    new_data = True

    txt_files = set([f for f in os.listdir(pageview_dir) if f.endswith('.txt')])

    for filename in filenames:
        except_counter = 0
        data_to_write = ""
        new_data = True
        print("Unzip file ", filename)

        if filename.replace(".gz", ".txt") in txt_files:
            print('    already unzipped and filtered')
            new_data = False
            count_views(data_to_write , None, filename)
            continue

        with gzip.open(os.path.join(pageview_dir, filename), 'rb') as infile:
            data = infile.readlines()

        # contains several different pages which are not utf-8 encoded and therefor through an exception
        # they are saved into an exception.p file and could be processed later if desired
        exceptions = []
        for line in data:
            line = line.decode('utf-8')
            split_line = line.split(' ')

            if split_line[0] in domains.keys():
                helper = urllib2.unquote(split_line[1].encode('utf-8'))

                try:
                    data_to_write += (split_line[0]+ " " + helper.decode('utf-8') + " " + split_line[2] + '\n')
                except:
                    except_counter += 1
                    exceptions.append(line)

        print("There have been ", except_counter, " exceptions thrown")
        with io.open(os.path.join(pageview_dir + "/results/", "exceptions.txt"), 'a', encoding = 'utf-8') as outfile:
            for line in exceptions:
                outfile.write(line)
        if new_data:
            print("Writing relevant domain entries into txt file")
            with io.open(os.path.join(pageview_dir, filename.replace(".gz", ".txt")), 'w', encoding = 'utf-8') as outfile:
                outfile.write(data_to_write)
        count_views(data_to_write, None, filename)



# # counting of page views # #

def count_views(view_list, page_id, filename):
    # for counting views of one single page (may contain an error, very old)
    if page_id is not None:
        #print("We got mail: ", page_id)
        if len(view_list) == 0:
            #print("Load txt file")
            for filename in filenames:
                with io.open(os.path.join(pageview_dir, filename), 'rb', encoding = 'utf-8') as infile:
                    view_list = infile.readlines()
        for line in view_list:

            if line.split(" ")[0] == id_per_title[page_id]:
                print(line)
                views_per_title[id_per_title[page_id]] += int(line.split(" ")[1])
        print("Views for ", id_per_title[page_id], ": ", views_per_title[id_per_title[page_id]])

    else:
        if len(view_list) == 0:
            print("Load txt files")
            with io.open(os.path.join(pageview_dir, filename.replace('.gz', '.txt')), 'r', encoding = 'utf-8') as infile:
                view_list = infile.readlines()

        for view_line in view_list:
            view_split = view_line.split(" ")

            try:
                replaced = view_split[1].replace("\'", "\\'")
                views_per_title[view_split[0] + " " + replaced] += int(view_split[2])
            except:
                pass


# # writing of mapping (old function, not used!) # #

def write_mapping(perform):
    if perform:
        if not os.path.exists(pageview_dir + "/results"):
            os.makedirs(pageview_dir + "/results")
        mapfiles = {}
        counters = {}

        for domain in domains.keys():
            mapfiles[(domain[:2] + domains[domain] + "_mapping.txt")] = io.open(os.path.join(pageview_dir + "/results", (domain[:2] + domains[domain] + "_mapping.txt")), 'w', encoding = 'utf-8')
            counters[domain[:2] + domains[domain]] = 0

        for key in views_per_title.keys():
            mapfiles[(key[:2] + domains[key[:4]] + "_mapping.txt")].write(str(counters[key[:2] + domains[key[:4]]]) + " " + id_per_title[key][4:] + "\n")
            counters[key[:2] + domains[key[:4]]] += 1



# # writing views into pickle and txt files # #

def write_views():

    print("Write results into files")
    if not os.path.exists(pageview_dir + "/results"):
            os.makedirs(pageview_dir + "/results")
    outfiles = {}
    mapfiles = {}

    # contains dicts of view counts (one dict for each domain)
    dom_dicts = {}

    # initialising of dicts
    for domain in domains.keys():

        current_domain = domain[:2] + domains[domain]
        new_direct = pageview_dir + "/results/" + current_domain + "/"
        if not os.path.exists(new_direct):
            os.makedirs(new_direct)

        outfiles[(current_domain + "_results.txt")] = io.open(os.path.join(new_direct, ("results.txt")), 'w', encoding = 'utf-8')
        mapfiles[(current_domain + "_mapping.txt")] = io.open(os.path.join(new_direct, ("mapping.txt")), 'w', encoding = 'utf-8')
        dom_dicts[current_domain] = {}

    # insert view counts into related dom_dict
    for key in views_per_title.keys():
        dom_dicts[key[:2] + domains[key[:4]]][key] = views_per_title[key]

    # write view counts into numpy array and write it into pickle files
    for domain in domains.keys():
        current_domain = domain[:2] + domains[domain]
        view_counts = np.zeros((len(dom_dicts[current_domain]), 2))

        # for new mapping
        counter = 0

        for key in dom_dicts[current_domain].keys():
            outfiles[(current_domain + "_results.txt")].write(str(counter) + " " + key[5:] + " " + str(views_per_title[key]) + "\n")
            mapfiles[(current_domain + "_mapping.txt")].write(str(counter) + " " + id_per_title[key][4:] + "\n")
            view_counts[counter, 0] = counter
            view_counts[counter, 1] = dom_dicts[current_domain][key]

            counter += 1

        # normalizing of view counts
        view_counts[:,1] = view_counts[:, 1] / np.sum(view_counts[:, 1])

        write_pickle(os.path.join(pageview_dir + "/results/" + current_domain + "/", "view_counts.p"), view_counts)




if __name__ == '__main__':
    download(False)

    download_other_files(False)
    make_txt_files()
    create_dicts()

    find_domain_entries()

    #write_mapping(True)
    write_views()
