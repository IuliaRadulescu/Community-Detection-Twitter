import json
import numpy as np
import tweepy
import time
from datetime import datetime
import mongoConnect
import sys
import pprint

class TwitterScraper:

	def __init__(self, credentialsFile):

		self.credentialsFile = credentialsFile

		self.connectTwitter()

	def connectTwitter(self):

		with open(self.credentialsFile) as json_file:
			data = json.load(json_file)
			consumerKey = data['CONSUMER_KEY']
			consumerSecret = data['CONSUMER_SECRET']
			accessToken = data["ACCESS_TOKEN"]
			accessTokenSecret = data["ACCESS_SECRET"]

		auth = tweepy.AppAuthHandler(consumerKey, consumerSecret)
		self.api = tweepy.API(auth, wait_on_rate_limit=False)

	def formatTweet(self, tweet):

		tweetJson = tweet._json

		# pprint.pprint(tweetJson)

		if (hasattr(tweet, "retweeted_status")):
			fullText = tweet.retweeted_status.full_text
		else:
			fullText = tweet.full_text

		if ("user_mentions" in tweetJson):
			userMentions = tweetJson["user_mentions"]
		else:
			userMentions = []

		if ("hashtags" in tweetJson):
			hashtags = tweetJson["hashtags"]
		else:
			hashtags = []

		return {
			"tweetId" : tweetJson["id"],
			"tweetCreated" : datetime.strptime(tweetJson["created_at"], "%a %b %d %H:%M:%S %z %Y"), # see formats here: https://www.journaldev.com/23365/python-string-to-datetime-strptime
			"text" : fullText,
			"retweetCount" : tweetJson["retweet_count"],
			"favoriteCount": tweetJson["favorite_count"],
			"user": {
				"userId" : tweetJson["user"]["id"],
				"userName" : tweetJson["user"]["name"],
				"userFollowersCount" : tweetJson["user"]["followers_count"],
				"userFriendsCount" : tweetJson["user"]["friends_count"]
			},
			"entities": {
				"userMentions": userMentions,
				"hashtags": hashtags
			},
			"retweeted": tweetJson["retweeted"]
		}

	'''
	Scrape communities based on the tweet-retweet relation
	'''
	def scrapeCommunities(self, location, numberOfTweetsPerRequest, totalNumberOfTweets, resultType):

		tweetsList = []

		counter = 0
		
		while counter < totalNumberOfTweets:
			print(counter)
			try:
				# get only retweets
				if (counter == 0):
					cursor = tweepy.Cursor(self.api.search, q='* filter:retweets', geocode=location, count = numberOfTweetsPerRequest, result_type = resultType, tweet_mode="extended")
				else:
					# use max_id to pick up where we left off
					cursor = tweepy.Cursor(self.api.search, q='* filter:retweets', geocode=location, count = numberOfTweetsPerRequest, result_type = resultType, max_id = formattedTweet["tweetId"], tweet_mode="extended") 

				for tweet in cursor.items(numberOfTweetsPerRequest):

					formattedTweet = self.formatTweet(tweet)

					# add parent tweet to the list

					parentTweet = self.formatTweet(tweet.retweeted_status)
					tweetsList.append(parentTweet)

					formattedTweet["retweetOf"] = {
						"tweetId": None,
						"userId": None
					}

					formattedTweet["retweetOf"]["tweetId"] = formattedTweet["tweetId"]
					formattedTweet["retweetOf"]["userId"] = formattedTweet["user"]["userId"]

					tweetsList.append(formattedTweet)

					counter = counter + 1					

			except Exception as exp: # ConnectionResetError: [Errno 104] Connection reset by peer - Twitter cuts off the connection so you need to reconnect
				print(exp)
				time.sleep(60)
				self.connectTwitter()

		return tweetsList

class MongoDBWriter:

	def __init__(self, dbName):
		self.mongoDriver = mongoConnect.MongoDBConnector(dbName)

	def writeTweetsToDB(self, tweetList, collection):
		self.mongoDriver.insertMany(collection, tweetList)


twitterScraper = TwitterScraper('twitterCredentials.json')
tweets = twitterScraper.scrapeCommunities("40.730610,-73.935242,50km", 300, 300, "recent") #popular sometimes does not respect count, see https://github.com/tweepy/tweepy/issues/560
# print(tweets)
mongoDBWriter = MongoDBWriter("TwitterCommunityDetection")
mongoDBWriter.writeTweetsToDB(tweets, "new_york")