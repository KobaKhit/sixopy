import tweepy
from textblob import TextBlob
from datetime import datetime, timedelta
import re
import json
import sixopy.utils as u
import sys
from collections import OrderedDict

# states
states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
states = [x for x in map(lambda x:x.lower(),states)]

# lookup arrays to convert user's timezone to a country
TimeZones = ['International Date Line West','Midway Island','American Samoa','Hawaii','Alaska','Pacific Time (US & Canada)','Tijuana','Mountain Time (US & Canada)','Arizona','Chihuahua','Mazatlan','Central Time (US & Canada)','Saskatchewan','Guadalajara','Mexico City','Monterrey','Central America','Eastern Time (US & Canada)','Indiana (East)','Bogota','Lima','Quito','Atlantic Time (Canada)','Caracas','La Paz','Santiago','Newfoundland','Brasilia','Buenos Aires','Montevideo','Georgetown','Greenland','Mid-Atlantic','Azores','Cape Verde Is.','Dublin','Edinburgh','Lisbon','London','Casablanca','Monrovia','UTC','Belgrade','Bratislava','Budapest','Ljubljana','Prague','Sarajevo','Skopje','Warsaw','Zagreb','Brussels','Copenhagen','Madrid','Paris','Amsterdam','Berlin','Bern','Rome','Stockholm','Vienna','West Central Africa','Bucharest','Cairo','Helsinki','Kyiv','Riga','Sofia','Tallinn','Vilnius','Athens','Istanbul','Minsk','Jerusalem','Harare','Pretoria','Kaliningrad','Moscow','St. Petersburg','Volgograd','Samara','Kuwait','Riyadh','Nairobi','Baghdad','Tehran','Abu Dhabi','Muscat','Baku','Tbilisi','Yerevan','Kabul','Ekaterinburg','Islamabad','Karachi','Tashkent','Chennai','Kolkata','Mumbai','New Delhi','Kathmandu','Astana','Dhaka','Sri Jayawardenepura','Almaty','Novosibirsk','Rangoon','Bangkok','Hanoi','Jakarta','Krasnoyarsk','Beijing','Chongqing','Hong Kong','Urumqi','Kuala Lumpur','Singapore','Taipei','Perth','Irkutsk','Ulaanbaatar','Seoul','Osaka','Sapporo','Tokyo','Yakutsk','Darwin','Adelaide','Canberra','Melbourne','Sydney','Brisbane','Hobart','Vladivostok','Guam','Port Moresby','Magadan','Srednekolymsk','Solomon Is.','New Caledonia','Fiji','Kamchatka','Marshall Is.','Auckland','Wellington',"Nuku'alofa",'Tokelau Is.','Chatham Is.','Samoa','Europe/London','America/New_York','EST']
Country = ['United States Minor Outlying Islands','United States Minor Outlying Islands','American Samoa','United States','United States','United States','Mexico','United States','United States','Mexico','Mexico','United States','Canada','Mexico','Mexico','Mexico','Guatemala','United States','United States','Colombia','Peru','Peru','Canada','Venezuela','Bolivia','Chile','Canada','Brazil','Argentina','Uruguay','Guyana','Greenland','South Georgia and the South Sandwich Islands','Portugal','Cape Verde','Ireland','United Kingdom','Portugal','United Kingdom','Morocco','Liberia','#N/A','Serbia','Slovakia','Hungary','Slovenia','Czech Republic','Bosnia and Herzegovina','Macedonia','Poland','Croatia','Belgium','Denmark','Spain','France','Netherlands','Germany','Germany','Italy','Sweden','Austria','Algeria','Romania','Egypt','Finland','Ukraine','Latvia','Bulgaria','Estonia','Lithuania','Greece','Turkey','Belarus','Israel','Zimbabwe','South Africa','Russia','Russia','Russia','Russia','Russia','Kuwait','Saudi Arabia','Kenya','Iraq','Iran','Oman','Oman','Azerbaijan','Georgia','Armenia','Afghanistan','Russia','Pakistan','Pakistan','Uzbekistan','India','India','India','India','Nepal','Bangladesh','Bangladesh','Sri Lanka','Kazakhstan','Russia','Myanmar','Thailand','Thailand','Indonesia','Russia','China','#N/A','Hong Kong','China','Malaysia','Singapore','Taiwan','Australia','Russia','Mongolia','South Korea','Japan','Japan','Japan','Russia','Australia','Australia','Australia','Australia','Australia','Australia','Australia','Russia','Guam','Papua New Guinea','Russia','Russia','Solomon Islands','New Caledonia','Fiji','Russia','Marshall Islands','New Zealand','New Zealand','Tonga','Tokelau','New Zealand','Samoa','United Kingdom','United States','United States']


# Constants
# Tweets column names and types. 34 in total
Colnames = 'text,sentiment,sentiment score,created at,favorite count,retweet count,favorited,retweeted,type,url,thumbnail,id str,lang,source,truncated,user description,user favourites count,user followers count,user friends count,user statuses count,user id str,user location,user state,user city,user name,user profile image url,user screen name,user time zone,user timezone country,country code,long,latt,hashtags,urls'.split(',')
Coltypes = ['varchar(MAX)','varchar(MAX)','float','datetime','float','float','bit','bit','varchar(255)','varchar(255)','varchar(MAX)','varchar(255)',\
    'varchar(255)','varchar(255)','bit','varchar(MAX)','float','float','float','float','varchar(255)','varchar(255)',\
    'varchar(255)','varchar(255)','varchar(255)','varchar(MAX)','varchar(255)','varchar(255)','varchar(MAX)','varchar(MAX)','float','float','varchar(255)','varchar(255)']


def cleanhtml(raw_html):
  # remove html tags from a string
  # Args:
  # - raw_html - string.
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return(cleantext)

def get_state_city(location):
  # extracts state and city if state is a US state
  # Args:
  #  - location - string (Phoenix, AZ)
  statecity = ['','']
  if location != None and ',' in location:
    location = location.lower()
    locsplit = location.split(',')
    if re.sub('[\s+]', '', locsplit[1]) in states:
          statecity = [re.sub('[\s+]', '', locsplit[1]), locsplit[0]]
    elif re.sub('[\s+]', '', locsplit[0]) in states:
          statecity = [re.sub('[\s+]', '', locsplit[0]), locsplit[1]]

  return(statecity)

def tweet_json_to_dict(tweet):
  # Converts twitter api json response to a dictionary
  # https://dev.twitter.com/overview/api/entities-in-twitter-objects#extended_entities

  # Response structures differ between streaming and get_status
  if type(tweet) is tweepy.models.Status: # get_status response
    tw = json.loads(json.dumps(tweet._json))
    tweet = TextBlob(tw["full_text"])
  elif type(tweet) is str: # streaming response
    tw = json.loads(tweet)
    tweet = TextBlob(tw["text"])

  # determine if sentiment is positive, negative, or neutral
  if tweet.sentiment.polarity < 0:
      sentiment = "negative"
  elif tweet.sentiment.polarity == 0:
      sentiment = "neutral"
  else:
      sentiment = "positive"

  # get fields
  fields = OrderedDict()
  fields['text'] = tweet.raw
  fields['sentiment'] = sentiment
  fields['sentiment_score'] = tweet.sentiment.polarity
  fields['created_at'] = datetime.strptime(tw["created_at"],'%a %b %d %H:%M:%S +0000 %Y') - timedelta(hours=4) # convert to eastern time
  fields['favorite_count'] = tw["favorite_count"]
  fields['retweet_count'] = tw["retweet_count"]
  fields['favorited'] = tw["favorited"]
  fields['retweeted'] = tw["retweeted"]
  fields['type'] = tw['extended_entities']['media'][0]['type'] if 'extended_entities' in tw.keys()   else None
  fields['url'] = tw['extended_entities']['media'][0]['url'] if 'extended_entities' in tw.keys() else None
  fields['thumbnail'] = tw['extended_entities']['media'][0]['media_url_https'] if 'extended_entities' in tw.keys()  else None
  fields['id_str'] = tw["id_str"]
  fields['lang'] = tw["lang"]
  fields['source'] = cleanhtml(tw["source"])
  fields['truncated'] = tw["truncated"]
  fields['user_description'] = tw["user"]["description"]
  fields['user_favourites_count'] = tw["user"]["favourites_count"]
  fields['user_followers_count'] = tw["user"]["followers_count"]
  fields['user_friends_count'] = tw["user"]["friends_count"]
  fields['user_statuses_count'] = tw["user"]["statuses_count"]
  fields['user_id_str'] = tw["user"]["id_str"]
  fields['user_location'] = tw["user"]["location"]
  fields['user_state'] = get_state_city(tw["user"]["location"])[0]
  fields['user_city'] = get_state_city(tw["user"]["location"])[1]
  fields['user_name'] = tw["user"]["name"]
  fields['user_profile_image_url'] = tw["user"]["profile_image_url"]
  fields['user_screen_name'] = tw["user"]["screen_name"]
  fields['user_time_zone'] = tw["user"]["time_zone"]

  # create tinyurl
  # ur = "https://twitter.com/"+ tw["user"]["screen_name"] + "/status/" + tw["id_str"]
  # if fields['url'] is '': fields['url'] = u.urlshortener(ur)

   # convert user timezone into country
  user_timezone_country = ''
  if fields['user_time_zone'] in TimeZones:
      index = TimeZones.index(fields['user_time_zone'])
      user_timezone_country= Country[index]
  fields['user_timezone_country'] = user_timezone_country

  fields['country_code'] = tw['place']['country_code'] if tw['place'] != None else None
  fields['long'] = tw['coordinates']['coordinates'][0] if tw['coordinates'] != None else None
  fields['latt'] = tw['coordinates']['coordinates'][1] if tw['coordinates'] != None else None

  # convert hashtags array into comma separated string
  hashtags = tw["entities"]["hashtags"]
  hashtags_str = ""
  for i in range(0,len(hashtags),1):
      hashtags_str = hashtags_str + hashtags[i]["text"]
      if i + 1 < len(hashtags):
          hashtags_str = hashtags_str + ","
  fields['hashtags'] = hashtags_str

  # convert urls into comma separated string
  urls = tw["entities"]["urls"]
  urls_str = ""
  for i in range(0,len(urls),1):
      urls_str = urls_str + urls[i]["url"]
      if i + 1 < len(urls):
          urls_str = urls_str + ","
  fields['urls'] = urls_str

  # append to list
  # row = dict(zip(Colnames2,[created_at,favorite_count,retweet_count,favorited,retweeted,typee,url,thumbnail,id_str,lang,source,truncated,user_description,user_favourites_count,
  #                user_followers_count,user_friends_count,user_id_str,user_location,user_state,user_city,user_name,user_profile_image_url,user_screen_name,user_statuses_count,user_time_zone,user_timezone_country,hashtags_str,urls_str,tweet_text,tweet.sentiment.polarity,sentiment, country_code,longi,latt]))

  return(fields)


def tweets_json_to_list(tweets):
    outtweets = []
    for tw in tweets:
      outtweets.append(tweet_json_to_dict(tw))
    return(outtweets)


class Tw(object):
  """docstring for ClassName"""
  def __init__(self,consumer_key,consumer_secret,access_token,access_secret):
    super().__init__() # http://stackoverflow.com/questions/576169/understanding-python-super-with-init-methods

    # authorize twitter, initialize tweepy
    self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    self.auth.set_access_token(access_token, access_secret)
    self.api = tweepy.API(self.auth)

  def get_all_user_tweets(self,screen_name,**kwargs):
    # Twitter only allows access to a users most recent 3240 tweets with this method
    api = self.api

    # initialize a list to hold all the tweepy Tweets
    alltweets = []  
    
    # make initial request for most recent tweets (200 is the maximum allowed count)
    # new_tweets = api.user_timeline(screen_name = screen_name, since_id = oldid, count=200)
    new_tweets = api.user_timeline(screen_name = screen_name, count = 200,tweet_mode='extended', **kwargs)
    
    # save most recent tweets
    alltweets.extend(new_tweets)
    
    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    
    # keep grabbing tweets until there are no tweets left to grab
    sys.stdout.write("getting tweets before {}\n".format(oldest))
    sys.stdout.write('\r')
    while len(new_tweets) > 0:
      
      #all subsiquent requests use the max_id param to prevent duplicates
      new_tweets = api.user_timeline(screen_name = screen_name, max_id=oldest, count = 200, tweet_mode='extended', **kwargs)
      
      #save most recent tweets
      alltweets.extend(new_tweets)
      
      #update the id of the oldest tweet less one
      oldest = alltweets[-1].id - 1
      
      
      sys.stdout.write("..{} tweets downloaded so far".format(len(alltweets)))
      sys.stdout.flush()
    
    return(tweets_json_to_list(alltweets))


def main():
  #Twitter API credentials
  consumer_key = 'your_key'
  consumer_secret = 'your_csecret'
  access_token = 'your_token'
  access_secret = 'your_asecret'

  # my creds
  f = u.parse_config(r'C:\Users\kkhitalishvili\Desktop\creds.txt')
  consumer_key, consumer_secret, access_token, access_secret = dict(f.items('twitter')).values()


  tw = Tw(consumer_key,consumer_secret,access_token,access_secret)
  w = tw.get_all_user_tweets('sixers', since_id='865580741693919232')
  print(w[0])

  

if __name__=='__main__':
  main()