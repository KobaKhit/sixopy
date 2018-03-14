from sixopy.tw import Tw
from sixopy.bq import bQ
import pandas as pd
import numpy as np

import os
from tqdm import tqdm

def main():
	#Twitter API credentials
	consumer_key = ''
	consumer_secret = ''
	access_token = ''
	access_secret = ''
		
	creds = r"Kobas Data-5c6ec97a9dae.json"
	bq = bQ(creds)

	newest_post_id_sixers = bq.query_sync('''
	SELECT id_str FROM [denmark-house-sales:social_data.sixers_twitter] 
	where user_screen_name = 'sixers'
	order by created_at DESC LIMIT 1''')[0]['id_str']

	newest_post_id_njdevils = bq.query_sync('''
	SELECT id_str FROM [denmark-house-sales:social_data.sixers_twitter] 
	where user_screen_name = 'NJDevils'
	order by created_at DESC LIMIT 1''')[0]['id_str']

	sinceids = {'NJDevils': newest_post_id_njdevils,'sixers':newest_post_id_sixers}

	tw = Tw(consumer_key,consumer_secret,access_token,access_secret)
	users = ['sixers']
	for user in users:
		w = tw.get_all_user_tweets(user,since_id=sinceids[user])
		w = pd.DataFrame(w)
		w['created_date'] = w['created_at'].dt.strftime('%Y-%m-%d')
		w = w.ix[w['created_date'] < '2017-12-11']
		w.to_json(user+'_tw_posts.json',orient='records')

		w = w.replace({np.nan: None})
		bq.stream_data('social_data','sixers_twitter',w.to_dict('records'))


	# Download for the league
	# users = pd.read_csv('social accounts.csv')['Twitter']
	# for u in tqdm(users):
	# 	w = tw.get_all_user_tweets(u)
	# 	w = pd.DataFrame(w)
	# 	w['created_date'] = w['created_at'].dt.strftime('%Y-%m-%d')
	# 	w = w.replace({np.nan: None})
	# 	w.to_json('nhl-twitter/'+u+'_tw_posts.json',orient='records')

	# files = os.listdir('nhl-twitter')
	# w = pd.read_json('nhl-twitter/'+files[0],orient='records')
	# she = bq.schema_from_df(w)

	# bq.delete_table('social_content','twitter')	
	# bq.create_table('social_content','twitter',she)	
	# su = 0
	# for f in files:
	# 	w = pd.read_json('nhl-twitter/'+f,orient='records')
	# 	w = w.replace({np.nan: None})
	# 	bq.stream_data('social_content','twitter',w.to_dict('records'))

if __name__ == '__main__':
  main()