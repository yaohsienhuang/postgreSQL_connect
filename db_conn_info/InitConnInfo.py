from py_dotenv import read_dotenv

class initConnInfo:
	def __init__(self,env_name):
		read_dotenv('db_conn_info/'+env_name+'.env')