import os
import pandas as pd
from tqdm import tqdm
from pprint import pprint
import numpy as np
from google.cloud import bigquery
from sixopy.utils import size_of_list

# Authenticate google client

class bQ(object):
	# https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html
	def __init__(self,creds):
		os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds
		self.client = bigquery.Client().from_service_account_json(creds)
		self.TIMEOUT_MS = 10000 # default 10 seconds

	
	def query_sync(self,query,**kwargs):
		request = self.client.run_sync_query(query)
		request.timeout_ms = self.TIMEOUT_MS
		request.run()

		columns = [f.name for f in request.schema]
		print("{} rows fetched from table.".format(request.total_rows))
		data = [dict(zip(columns,row)) for row in request.rows]

		return(data)

	def create_dataset(self, dataset_name):
		dataset = self.client.dataset(dataset_name)
		dataset.create()   

	def delete_dataset(self,dataset_name, force = False):
		dataset = self.client.dataset(dataset_name)

		if force:           # API request
			for t in dataset.list_tables():
				t.delete()
		dataset.delete()              # API request

		assert not dataset.exists() 
		print(dataset_name + ' is deleted.')

	@staticmethod
	def schema_from_df(df,types={}):
		s = pd.io.json.build_table_schema(df,index=False)
		for d in s['fields']:
			if d['type'] == 'number': d['type'] = 'float'
			if d['name'] in types:
				d['type'] = types[d['name']]
		s = [bigquery.SchemaField(p['name'],p['type']) for p in s['fields']]
		return(s)

	@staticmethod
	def schema_from_string(schema):
		s = [bigquery.SchemaField(x.split(':')[0],x.split(':')[1]) for x in schema.split(',')]
		return(s)

	def create_table(self,dataset_name,table_name,schema):

		dataset = self.client.dataset(dataset_name)
		table = dataset.table(table_name, schema)
		table.create()     

	def delete_table(self,dataset_name,table_name):

		dataset = self.client.dataset(dataset_name)
		table = dataset.table(table_name)

		# assert table.exists()       # API request
		table.delete()              # API request
		assert not table.exists() 
		print(dataset_name+'.'+table_name+' is deleted.')


	def schema(self,dataset_name, table_name):
		dataset = self.client.dataset(dataset_name)
		table = dataset.table(table_name)

		# Reload the table to get the schema.
		table.reload()

		return(table.schema)

	def stream_data(self,dataset_name, table_name,data):
		#
		# Args:
		# - data is a list of dictionaries
		dataset = self.client.dataset(dataset_name)
		table = dataset.table(table_name)

		# Reload the table to get the schema.
		table.reload()

		schema = [x.name for x in table.schema]
		data = [tuple(d[k] for k in schema) for d in data]

		errors = table.insert_data(data)

		if not errors:
			print('Loaded {} rows into {}:{}'.format(str(len(data)),dataset_name, table_name))
		else:
			print('Errors:')
			pprint(errors)

	@staticmethod
	def chunk_data(data):
		size = size_of_list(data)+1
		if(size>10):
			return(chunkify(data,round(size/9.5)))
		else:
			return(data)

def main():
	creds = r"C:\Users\kkhitalishvili\Desktop\sixopy2\sixopy\sixopy-test\Kobas Data-5c6ec97a9dae.json"
	bq = bQ(creds)

	# newest_post_time = bq.query_sync('''
	# 	SELECT created_time FROM [denmark-house-sales:social_data.sixers_facebook] 
	# 	order by created_time DESC LIMIT 1''')[0]['created_time']
	
	print(bq.schema('social_data','sixers_instagram'))





if __name__ == "__main__":
	main()