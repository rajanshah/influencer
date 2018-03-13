# -*- coding: utf-8 -*-


import json
import os
import sys

def json_reader(dirname, filename):
    file_path = os.path.join(dirname, filename)
    json_data = None
    with open(file_path, 'r') as in_file:
        json_data = json.load(in_file)
    return json_data


class JSONWriter(object):
    '''JSON Writer class to handle all the files written in JSON
    '''
    
    def __init__(self, dirname):
        self.dirname = dirname
        self.__setup_directory(self.dirname)

    def __setup_directory(self, _path):
        '''If the directory doesn't exist, then creates it'''

        if not os.path.exists(_path):
            os.makedirs(_path)

    def __get_file_path(self, file_name, folder):
        '''Creates the path for the file and returns the complete file path'''

        if folder:
            file_path = os.path.join(self.dirname, folder)
            self.__setup_directory(file_path)
            return os.path.join(file_path, file_name)
        return os.path.join(self.dirname, file_name)

    def write_json_content(self, file_name, data, folder=None):
        '''Writes the JSON date into the file'''

        file_path = self.__get_file_path(file_name, folder)
        with open(file_path, 'w') as out_file:
            json.dump(data, out_file)