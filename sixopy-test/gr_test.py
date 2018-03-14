from sixopy.gr import Gr
from sixopy.bq import bQ
import pandas as pd
import numpy as np

import os
from tqdm import tqdm
import json 

def main():
	creds = r"Kobas Data-5c6ec97a9dae.json"
	bq = bQ(creds)

	table = 'sixers_instagram'
	# sche = bq.schema('social_data',table)
	
	# newest_post_date_sixers = bq.query_sync('''
	# SELECT created_date FROM [denmark-house-sales:social_data.sixers_instagram] 
	# where user = 'sixers'
	# order by created_time DESC LIMIT 1''')[0]['created_date']

	# newest_post_date_njdevils = bq.query_sync('''
	# SELECT created_date FROM [denmark-house-sales:social_data.sixers_instagram] 
	# where user = 'njdevils'
	# order by created_time DESC LIMIT 1''')[0]['created_date']

	# sincedates = {'njdevils': newest_post_date_njdevils,'sixers':newest_post_date_sixers}

	s = Gr() # Initialize class
	users = ['sixers']
	s.username = 'sixers'

	with open('sixers\\sixers.json',encoding='utf8') as json_data:
		a = json.load(json_data)

	w = pd.DataFrame(s.create_list(a))
	w['created_time'] = pd.to_datetime(w['created_time'])
	w['created_date'] = w['created_time'].dt.strftime('%Y-%m-%d')
	sche = bq.schema_from_df(w,types={'created_time':'DATETIME'})
	print(w.dtypes)

	# recreate tables
	bq.delete_table('social_data',table)
	bq.create_table('social_data',table, schema = sche)
	for user in users:
		# w = s.get_all_user_posts(user) # get all posts metadata
		# w = pd.DataFrame(df)
		w.to_json(user+'_gr_posts.json',orient='records')
		# w = pd.read_json(user+'_gr_posts.json',orient='records',dtype=False)
		w = w.ix[w['created_date'] < '2017-12-11']
		w.to_json(user+'_gr_posts.json',orient='records')

		w = w.replace({np.nan: None})
		
		bq.stream_data('social_data','sixers_instagram',w.to_dict('records'))


	# Download for the league
	# users = pd.read_csv('social accounts.csv')['Instagram']
	# for u in tqdm(users):
	# 	w = s.get_all_user_posts(u) # get all posts metadata
	# 	w = pd.DataFrame(w)
	# 	w.to_json('nhl-instagram/'+u+'_gr_posts.json',orient='records')
	# 	w = pd.read_json('nhl-instagram/'+u+'_gr_posts.json',orient='records')
	# 	w['created_date'] = w['created_time'].dt.strftime('%Y-%m-%d')
	# 	w = w.replace({np.nan: None})
	# 	w.to_json('nhl-instagram/'+u+'_gr_posts.json',orient='records')

	# files = os.listdir('nhl-instagram')
	# w = pd.read_json('nhl-instagram/'+files[0],orient = 'records')
	# shema = bq.schema_from_df(w)

	# bq.create_dataset('social_content')
	# bq.delete_table('social_content','instagram')
	# bq.create_table('social_content','instagram',shema)

	# for f in os.listdir('nhl-instagram'):
	# 	w = pd.read_json('nhl-instagram/'+f,orient = 'records')
	# 	w = w.replace({np.nan: None})
	# 	bq.stream_data('social_content','instagram',w.to_dict('records'))

if __name__ == "__main__":
	main()