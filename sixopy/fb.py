from facepy import GraphAPI
import io
import dateutil.parser as dateparser
from datetime import timedelta
 
from datetime import datetime
 
import pandas as pd
from tqdm import tqdm
 
class Fb(object):
	def __init__(self,token,**kwargs):
		self.graph = GraphAPI(token,**kwargs)


	def get_field(self,dic,field):
		return(dic[field] if field in dic else None)

	def construct_dict(self,dic,fields):
		return(dict([(f,self.get_field(dic,f)) for f in fields]))

	def get_page_info(self,pageid):
		graph = self.graph
		fields = "id,name,about,fan_count,category,link"
		page = graph.get(str(pageid) + "?fields=" + fields)
		page = self.construct_dict(page,fields.split(','))
		return(page)


	def get_posts(self,userid,insights = True, **kwargs):


		graph = self.graph
		pages = graph.get(str(userid) + "/posts", **kwargs)

		# Get post ids
		postids = list([p['id'] for page in pages for p in page['data']])# get just the post ids (pageid_postid, ex.g. 52974817515_10156185473452516)
		# postids = ['52974817515_10156205319582516','52974817515_10156204804522516']

		# remove duplicate post ids
		# postids = list(filter(lambda x: x not in sinceids,postids))
	 
		# Get post info
		name = graph.get(str(userid) + "?fields=name")['name']
		fields = "id,created_time,caption,description,link,message,type,status_type,place,permalink_url"
		fields_data = "comments.limit(0).summary(true),shares,reactions.limit(0).summary(true)"
		fields_insights = "post_reactions_anger_total,post_reactions_sorry_total,post_reactions_haha_total,post_reactions_wow_total,post_reactions_love_total,post_reactions_like_total,post_impressions,post_impressions_unique,post_impressions_paid,post_impressions_paid_unique,post_impressions_organic,post_impressions_organic_unique,post_consumptions,post_consumptions_unique,post_engaged_users,post_video_avg_time_watched,post_video_complete_views_organic,post_video_complete_views_paid,post_video_views,post_video_views_unique,post_video_views_10s,post_video_views_10s_unique,post_video_length,post_video_view_time"
		nestedresp = "like,share,comment,hide_all_clicks,hide_clicks"
		varss = str(fields+','+fields_insights+","+nestedresp).split(",")
		vartitles = {} # variable titles defined by facebook
		posts = []
		missed = 0
		for d in tqdm(postids):
			try:
				# get post fields
				pinfo = graph.get(str(d) + "?fields=" + fields + "," + fields_data) # dict with the fields
				
				pinfo['shares'] = pinfo['shares']['count'] if 'shares' in pinfo else 0 
				pinfo['comments'] = pinfo['comments']['summary']['total_count']
				pinfo['reactions'] = pinfo['reactions']['summary']['total_count']
				pinfo['name'] = name
				pinfo['id'] = str(pinfo['id'])
				
				# get post insights
				if insights is True:
					varss = str(fields+','+fields_insights+","+nestedresp).split(",") + ['comments','shares','reactions','name']
					pinsights = graph.get(str(d) + "/insights?metric=" + fields_insights)
					vartitles.update(dict([(p['name'],p['title']) for p in pinsights['data']]))
					pinsights = dict([(p['name'], p['values'][0]['value']) for p in pinsights['data']])
			  
					# get insights from nested responses
					paction = graph.get(str(d) + "/insights?metric=" + "post_stories_by_action_type")['data'][0]['values'][0]['value']
					paction.update(graph.get(str(d) + "/insights?metric=" + "post_negative_feedback_by_type")['data'][0]['values'][0]['value'])
		 
					pinfo.update(pinsights) # combine post fields and insights
					pinfo.update(paction)
				else:
					varss = fields.split(',') + ['comments','shares','reactions','name']
	 
				# construct dictionary with post data
				dic = self.construct_dict(pinfo,varss)
				if insights == True:
					for i in nestedresp.split(','):
						if dic[i] is None:
							dic[i] = 0
				# get location
				if dic['place'] is not None: dic['place'] = dic['place']['name']
	 
				posts.append(dic) # append to list
			except Exception as e:
				import traceback
				traceback.print_exc()
				missed += 1
	 
		print("Missed " + str(missed) + "/" + str(len(postids)))
	 

		vartitles=dict(zip(varss,[v.replace('_',' ') for v in varss])) # add field titles with underscore replaced by space
		
		if insights == True:
			vartitles.update({'post_reactions_anger_total':'Anger Reactions','post_reactions_sorry_total':'Sorry Reactions','post_reactions_haha_total':'Haha Reactions','post_reactions_wow_total':'Wow Reactions','post_reactions_love_total':'Love Reactions','post_reactions_like_total':'Like Reactions','shares':'shares','comments':'comments','reactions':'reactions','name':'name','like':'Likes','share':'Shares Insights','comment':'Comments Insights','hide_all_clicks': "Hide all clicks",'hide_clicks':'Hide clicks'}) # add nested response titles
			vartitles.update({'post_impressions': 'Lifetime Post Total Impressions', 'post_impressions_unique': 'Lifetime Post Total Reach', 'post_impressions_paid': 'Lifetime Post Paid Impressions', 'post_impressions_paid_unique': 'Lifetime Post Paid Reach', 'post_impressions_organic': 'Lifetime Post Organic Impressions', 'post_impressions_organic_unique': 'Lifetime Post organic reach', 'post_consumptions': 'Lifetime Post Consumptions', 'post_consumptions_unique': 'Lifetime Post Consumers', 'post_engaged_users': 'Lifetime Engaged Users', 'post_video_avg_time_watched': 'Lifetime Average time video viewed', 'post_video_length': 'Lifetime Video length', 'post_video_complete_views_organic': 'Lifetime Organic watches at 95 percent', 'post_video_complete_views_paid': 'Lifetime Paid watches at 95 percent', 'post_video_views': 'Lifetime Total Video Views', 'post_video_views_unique': 'Lifetime Unique Video Views','post_video_views_10s':'Lifetime Total 10 Second Views','post_video_views_10s_unique':'Lifetime Unique 10 Second Views','post_video_view_time':'Lifetime Total Video View Time in MS'})
	 
		posts = [{vartitles[k].replace(' ','_').lower(): v for k, v in p.items()} for p in posts] # add names

		return(posts)

	 
def main():
	atoken = ''
	fb = Fb(atoken,version='2.9')
	print(fb.get_page_info(52974817515))
	return
	posts = fb.get_posts(52974817515,insights = True, page=True,since = 1508630400)
	# posts = [d for d in posts if datetime.strptime(d['created time'],'%Y-%m-%dT%H:%M:%S+0000') <= datetime.strptime('10/8/2017', '%m/%d/%Y')]
	pd.DataFrame(posts).to_json('fb_posts_test.json',orient='records')
	print("Posts FILTERED: ",len(posts))
	return
 
 
	# with io.open(r'C:\Users\kkhitalishvili\Desktop\fb-content\data-mining\backup\fb_posts.txt','w', encoding='utf-8') as file:
	#       file.write("\t".join(posts[0].keys()))
	#       file.write('\n')
	#       for row in posts:
	#           file.write("\t".join([str(val).replace('\n',' ') for val in row.values()]))
	#           file.write('\n')
 
 
	df = pd.DataFrame(posts)
	df.to_csv(r'C:\Users\kkhitalishvili\Desktop\social\social-content-report\sixers_fb2.csv')
	return
 
 
if __name__ == "__main__":
	main()
