import csv
import configparser


def read_config(config_file):
	''' read config file '''

	Config = configparser.ConfigParser()
	Config.read(config_file)

	return Config

def read_csv_file(file_path):
	with open(file_path) as csv_file:
		csvreader = csv.reader(csv_file)
		for security in csvreader:
			yield security[0]

