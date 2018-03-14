from instagram_scraper import InstagramScraper
# import instascrape
from datetime import datetime
import pandas as pd
import dateutil.parser as dateparser
import re
import json

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

class Gr(object):
	"""docstring for ClassName"""
	def __init__(self):
		super().__init__() # http://stackoverflow.com/questions/576169/understanding-python-super-with-init-methods
		# Scrape instagram
		self.scraper = InstagramScraper(usernames = ['sixers'], destination = './', login_user = None, login_pass = None, quiet = False,\
			maximum = 0, media_metadata = False, media_types = 'none', latest = False)
		self.username = None
	
	def create_list(self,mediaItems):
		# Create a data frame using Instagram API response
		# Args:
		# - mediaItems = list of media items as generated by the InstagramScraper.media_gen
		created_time = list(map(lambda post: datetime.fromtimestamp(int(post["taken_at_timestamp"])).strftime('%Y-%m-%d %H:%M:%S'), mediaItems))
		ptype = list(map(lambda post: post["__typename"].replace('Graph',''), mediaItems))
		caption = list(map(lambda post: str(post["edge_media_to_caption"]['edges'][0]['node']['text']) if post["edge_media_to_caption"]['edges'] else '', mediaItems))
		tags =  list(map(lambda post: post["tags"] if 'tags' in post else '', mediaItems))
		likes = list(map(lambda post: post["edge_media_preview_like"]["count"], mediaItems))
		comments = list(map(lambda post: post["edge_media_to_comment"]["count"], mediaItems))
		idd = list(map(lambda post: str(post["id"]), mediaItems))
		url = list(map(lambda post: 'https://www.instagram.com/p/'+post["shortcode"], mediaItems))
		video_views = list(map(lambda post: post["video_view_count"] if 'video_view_count' in post else 0, mediaItems))

		posts = [{'created_time':created_time[i], \
				  'type': ptype[i], \
				  'caption': caption[i], \
				  'likes': likes[i], \
				  'comments':comments[i], \
				  'id':idd[i], \
				  'url': url[i], \
				  'video_views': video_views[i], \
				  'user':self.username} for i in range(0,len(created_time))]

		return(posts)

	def get_all_user_posts(self, username):
		self.username = username
		# Get all posts metadata by the user
		m = list(self.scraper.query_media_gen({'id':username}))
		return(self.create_list(m))

def main():
	s = Gr() # Initialize class
	print()
	# a = s.get_all_user_posts('dostre') # get all posts metadata
	with open('sixers.json',encoding='utf8') as json_data:
		a = json.load(json_data)

	s.username = 'sixers'
	a = s.create_list(a) 
	pd.DataFrame(a).to_json("sixers-posts.json",orient='records')

if __name__ == "__main__":
	main()