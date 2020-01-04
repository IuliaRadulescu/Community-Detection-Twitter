import json
import tweepy

with open('twitterCredentials.json') as json_file:
	data = json.load(json_file)
	consumerKey = data['CONSUMER_KEY']
	consumerSecret = data['CONSUMER_SECRET']
	accessToken = data["ACCESS_TOKEN"]
	
auth = tweepy.AppAuthHandler(consumerKey, consumerSecret)

api = tweepy.API(auth)
for tweet in tweepy.Cursor(api.search, q='*', count=1, geocode="-32.916668,151.750000,100km", result_type="popular").items(1):
	json_formatted_str = json.dumps(tweet._json, indent=2)
	print(json_formatted_str)
