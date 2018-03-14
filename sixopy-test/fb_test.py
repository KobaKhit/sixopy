from sixopy.fb import Fb
from sixopy.bq import bQ
from datetime import datetime
import pandas as pd
import numpy as np

import os
from tqdm import tqdm

def main():
	creds = r"C:\Users\kkhitalishvili\Desktop\sixopy2\sixopy\sixopy-test\Kobas Data-5c6ec97a9dae.json"
	bq = bQ(creds)

	newest_post_datetime = bq.query_sync('''
	SELECT created_time FROM [denmark-house-sales:social_data.sixers_facebook] 
	order by created_time DESC LIMIT 1''')[0]['created_time']

	atoken = ''
	fb = Fb(atoken,version='2.9')
	posts = fb.get_posts(52974817515,page=True,since=newest_post_datetime.timestamp())

	# posts = [d for d in posts if datetime.strptime(d['created_time'],'%Y-%m-%dT%H:%M:%S+0000') <= datetime.strptime('10/8/2017', '%m/%d/%Y')]
	w = pd.DataFrame(posts)
	w.to_json('fb_posts.json',orient='records')
	w = pd.read_json('fb_posts.json',orient='records')
	w['created_date'] = w['created_time'].dt.strftime('%Y-%m-%d')
	w = w.ix[w['created_date'] < '2017-12-11']

	w = w.replace({np.nan: None})
	bq.stream_data('social_data','sixers_facebook',w.to_dict('records'))

	# download for the whole league
	# users = pd.read_csv('social accounts.csv')['Facebook']
	# since = datetime(2017, 11, 6).timestamp()
	# for u in tqdm(users):
	# 	w = fb.get_posts(u,insights = False, page=True,since=since)
	# 	w = pd.DataFrame(w)
	# 	w.to_json('nhl-facebook/'+w['name'].unique()[0]+'_fb_posts.json',orient='records')
	# 	w = pd.read_json('nhl-facebook/'+w['name'].unique()[0]+'_fb_posts.json',orient='records')
	# 	w['created_date'] = w['created_time'].dt.strftime('%Y-%m-%d')
	# 	w = w.replace({np.nan: None})
	# 	w.to_json('nhl-facebook/'+w['name'].unique()[0]+'_fb_posts.json',orient='records')

	# files = os.listdir('nhl-facebook')
	# w = pd.read_json('nhl-facebook/'+files[0],orient = 'records')
	# shema = bq.schema_from_df(w,types={'place':'string'})

	# # bq.create_dataset('social_content')
	# # bq.create_table('social_content','facebook',shema)

	# for f in os.listdir('nhl-facebook'):
	# 	w = pd.read_json('nhl-facebook/'+f,orient = 'records')
	# 	w = w.replace({np.nan: None})
	# 	bq.stream_data('social_content','facebook',w.to_dict('records'))

if __name__ == "__main__":
	main()