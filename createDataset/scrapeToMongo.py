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

		auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
		auth.set_access_token(accessToken, accessTokenSecret)

		self.api = tweepy.API(auth, wait_on_rate_limit=True)

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
			"retweetCount": tweetJson["retweet_count"],
			"retweeted": tweetJson["retweeted"]
		}

	def getFollowerListForUser(self, userId, followersIds):
		# get followers ids
		followerTweets = []
		cursorFollowersIds = tweepy.Cursor(self.api.followers_ids, id = userId).items()

		for followerId in cursorFollowersIds:
			followersIds.append(followerId)


	def scrapeCommunities(self, location, numberOfTweetsPerRequest, totalNumberOfTweets, resultType):

		tweetsList = []

		counter = 0
		
		while counter < totalNumberOfTweets:

			try:
				if (counter == 0):
					cursor = tweepy.Cursor(self.api.search, q='*', geocode=location, count = numberOfTweetsPerRequest, result_type = resultType, tweet_mode="extended")
				else:
					# use max_id to pick up where we left off
					cursor = tweepy.Cursor(self.api.search, q='*', geocode=location, count = numberOfTweetsPerRequest, result_type = resultType, max_id = formattedTweet["tweetId"], tweet_mode="extended") 

				for tweet in cursor.items(numberOfTweetsPerRequest):

					formattedTweet = self.formatTweet(tweet)

					print("got tweet number " + str(counter))
					print(formattedTweet["user"]["userFollowersCount"])
					
					formattedTweet["user"]["userFollowersIds"] = []
					
					# get follower list and followers tweets
					self.getFollowerListForUser(formattedTweet["user"]["userId"], formattedTweet["user"]["userFollowersIds"])
					
					tweetsList.append(formattedTweet)

					counter = counter + 1

			except: # ConnectionResetError: [Errno 104] Connection reset by peer - Twitter cuts off the connection so you need to reconnect
				time.sleep(60)
				self.connectTwitter()

		return tweetsList

	def fetchTweetForUser(self, userId, numberOfUserTweets):

		tweetsList = []

		try:
			for tweet in tweepy.Cursor(self.api.user_timeline, id=userId, tweet_mode="extended").items(numberOfUserTweets):
				formattedTweet = self.formatTweet(tweet)
				tweetsList.append(formattedTweet)
		except tweepy.TweepError:
			print ("tweepy.TweepError")
		except:
			e = sys.exc_info()[0]
			print ("Error: %s", e)
		return tweetsList

class MongoDBWriter:

	def __init__(self, dbName):
		self.mongoDriver = mongoConnect.MongoDBConnector(dbName)

	def writeTweetsToDB(self, tweetList, collection):
		self.mongoDriver.insertMany(collection, tweetList)


twitterScraper = TwitterScraper('twitterCredentials.json')
tweets = twitterScraper.scrapeCommunities("-34.285999,150.545999,10km", 15, 240, "recent") #popular sometimes does not respect count, see https://github.com/tweepy/tweepy/issues/560
# print(tweets)
mongoDBWriter = MongoDBWriter("TwitterCommunityDetection")
mongoDBWriter.writeTweetsToDB(tweets, "australiaFires_4_1_2019_12_00")