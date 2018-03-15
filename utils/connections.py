import rethinkdb as r

def get_rethink_connection(props):
	''' get rethink db connection  '''

	rethink_conn = r.connect(host=props.get('RETHINKDB', 'RETHINK_HOST'),\
								port=props.get('RETHINKDB', 'RETHINK_PORT'),\
								db=props.get('RETHINKDB', 'RETHINK_DB'),\
								user=props.get('RETHINKDB', 'RETHINK_USER'),\
								password=props.get('RETHINKDB', 'RETHINK_PASSWORD'),\
								timeout=int(props.get('RETHINKDB', 'RETHINK_TIMEOUT')))
	return rethink_conn


