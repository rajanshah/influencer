# -*- coding: utf-8 -*-

import base64
import csv
import datetime
import os
import time

import lxml.html

from ConfigParser import ConfigParser


def read_config(config_file):
    '''read config file'''

    config = ConfigParser()
    config.read(config_file)
    return config

def get_tree(source):
    '''Returns the lxml element of the source'''

    return lxml.html.fromstring(source)

def setup_directory(path):
    '''If the directory doesn't exist, then creates it'''
    if not os.path.exists(path):
        os.makedirs(path)

def read_csv_file(file_path):
    with open(file_path) as csv_file:
        csvreader = csv.reader(csv_file)
        for security in csvreader:
            yield security[0]

def read_csv_dict_file(file_path):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as in_file:
            reader = csv.DictReader(in_file)
            for row in reader:
                yield row


def write_csv_file(path, file_name ,fieldnames, datas):
    setup_directory(path)
    file_path = os.path.join(path, file_name)
    with open(file_path, "w") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()
        for data in datas:
            writer.writerow(data)

def encode_base64(content):
    return base64.b64encode(content)

def time_delta(start):
    delta = time.time() - start
    m, s = divmod(int(delta), 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)

def is_tomorrow():
    now = datetime.datetime.now()
    return now.hour == 0

