import json
import numpy as np
import tweepy
import time
from datetime import datetime
import mongoConnect
import sys

class TwitterScraper:

	def __init__(self, credentialsFile):

		with open(credentialsFile) as json_file:
			data = json.load(json_file)
			consumerKey = data['CONSUMER_KEY']
			consumerSecret = data['CONSUMER_SECRET']
			accessToken = data["ACCESS_TOKEN"]
			accessTokenSecret = data["ACCESS_SECRET"]

		auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
		auth.set_access_token(accessToken, accessTokenSecret)

		self.api = tweepy.API(auth)

	def formatTweet(self, tweet):

		tweetJson = tweet._json

		if (hasattr(tweet, "retweeted_status")):
			fullText = tweet.retweeted_status.full_text
		else:
			fullText = tweet.full_text

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
			}
		}

	def getFollowerListForUser(self, userId, followersCount, followersIds, tweetsList):
		# get followers ids
		followerTweets = []
		cursorFollowersIds = tweepy.Cursor(self.api.followers_ids, id = userId).items(followersCount)

		for followerId in cursorFollowersIds:
			followersIds.append(followerId)
			followerTweets = self.fetchTweetForUser(followerId, 1)

			for followerTweet in followerTweets:
				tweetsList.append(followerTweet)


	def scrapeCommunities(self, location, resultType, numberOfPrimaryTweets, maxNumberOfFollowers):

		tweetsList = []
		
		for tweet in tweepy.Cursor(self.api.search, q='*', geocode=location, result_type=resultType, tweet_mode="extended").items(numberOfPrimaryTweets):

			formattedTweet = self.formatTweet(tweet)

			print(formattedTweet["user"]["userFollowersCount"])
			
			# get maximum n followers
			followersCount = min(formattedTweet["user"]["userFollowersCount"], maxNumberOfFollowers)
			formattedTweet["user"]["userFollowersIds"] = []
			
			# get follower list and followers tweets
			self.getFollowerListForUser(formattedTweet["user"]["userId"], followersCount, formattedTweet["user"]["userFollowersIds"], tweetsList)
			
			tweetsList.append(formattedTweet)
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
tweets = twitterScraper.scrapeCommunities("-32.916668,151.750000,100km", "mixed", 3, 10) #popular sometimes does not respect count, see https://github.com/tweepy/tweepy/issues/560
# print(tweets)
mongoDBWriter = MongoDBWriter("TwitterCommunityDetection")
mongoDBWriter.writeTweetsToDB(tweets, "mongoStaticCommunity")