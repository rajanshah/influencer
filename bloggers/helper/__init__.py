import argparse
import datetime
import os

import hmac
from hashlib import sha1

import rethinkdb as r

import ConfigParser


from kitchen.text.converters import to_unicode

from pytz import timezone

def parse_arguments(program_name):
	''' parse arguments '''

	# parse arguments
	parser = argparse.ArgumentParser(description=program_name)
	parser.add_argument('--root_dir', action='store', dest='root_dir',\
		required=True, help='root directory')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config')
	parser.add_argument('--seed_id', action='store', dest='seed_id',\
		required=True, help='seed_id')
	parser.add_argument('--interval', action='store', dest='interval',\
		required=True, help='config')
	parser.add_argument('--from_dt', action='store', dest='from_dt',\
		required=False, help='config')
	parser.add_argument('--to_dt', action='store', dest='to_dt',\
		required=False, help='config')
	parser.add_argument('--date', action='store', dest='date',\
		required=False, help='config')
	result = parser.parse_args()
	return result

def is_empty(path):
	return os.stat(path).st_size==0

def read_config(config_file):
	''' read config file '''

	Config = ConfigParser.ConfigParser()
	Config.read(config_file)

	return Config

def list_files(root_dir):
	return [os.path.join(x[0],y) for x in os.walk(root_dir) for y in x[2]]

def get_today():
	''' get today '''

	now_time = datetime.datetime.now(timezone('US/Eastern'))
	return now_time.date()
	

def adjust_today(y, m, d):
	''' adjust date according to offset '''

	# get today's date
	dt = get_today()

	# adjust according to delta
	try:
		adjusted_date = datetime.date(dt.year + y, dt.month + m , dt.day + d)
	except:
		raise Exception('invalid offset')	

	return adjusted_date


def generate_hmac_key(secret_shared_key, raw_text):
	''' generate hmac key '''
	
	digest_maker = hmac.new(secret_shared_key, raw_text, sha1)
	return digest_maker.digest().encode("base64").rstrip("\n")	


def cleanup_text(message):
	''' clean-up text '''

	return to_unicode(message)
	try:
		message = message.decode('utf-8')
	except:
		try:
			message = message.decode('ISO-8859-1')
		except:
			try:
				message = message.decode('windows-1252')
			except:
				message = None

	return message

def curate_text(content):
	''' curate text content '''
	
	return to_unicode(content, nonstring='simplerepr').encode('ascii','ignore')
