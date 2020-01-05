import mongoConnect
import igraph
import louvain
import collections

class CommunityDetection():

	def __init__(self):

		self.g = igraph.Graph()

	def buildGraph(self, edgesDict):

		allNodesList = list(edgesDict.keys())

		print(len(allNodesList))

		for nodesList in list(edgesDict.values()):
			allNodesList = allNodesList + nodesList

		self.g.add_vertices(max(allNodesList)+1)

		print(max(allNodesList)+1)

		edges = []

		for key, value in list(zip(edgesDict, edgesDict.values())):
			for node in value:
				edges.append((key, node))

		self.g.add_edges(edges)

		self.g.write_gml("new_york.gml")

	def applyLouvain(self):

		partition = louvain.find_partition(self.g, louvain.ModularityVertexPartition)
		igraph.plot(partition)

		#print(partition[0])

	'''
	Transform tweet - retweet in order to be fed into igraph
	Use Adjacency List 
	'''
	def preprocessDataset(self, twitterDatset):

		nodeId = 0
		edgesDict = collections.defaultdict(list)
		userIds2nodeIds = {}

		for element in twitterDatset:
			userId = element["user"]["userId"]
			parentId = element["retweetOf"]["userId"]

			if userId not in list(userIds2nodeIds.keys()):
				userIds2nodeIds[userId] = nodeId
				nodeId = nodeId + 1

			if parentId not in list(userIds2nodeIds.keys()):
				userIds2nodeIds[parentId] = nodeId
				nodeId = nodeId + 1

			edgesDict[userIds2nodeIds[userId]].append(userIds2nodeIds[parentId])

		return edgesDict


class MongoDBReader():

	def __init__(self, dbName):
		self.mongoDriver = mongoConnect.MongoDBConnector(dbName)

	def readWholeTwitterDataset(self, collection):
		tweetsDataset = []

		tweetsCursor = self.mongoDriver.getRecords(collection, {}, {})

		for tweet in tweetsCursor:
			tweetsDataset.append(tweet)

		return tweetsDataset

	def readTwitterUserTweetRetweet(self, collection):
		tweetsConnections = []

		tweetsConnectionsCursor = self.mongoDriver.getRecords(collection, {"retweetOf": {"$ne": None}}, {'_id': 0, 'user.userId':1, 'retweetOf.userId':1})

		for tweet in tweetsConnectionsCursor:
			tweetsConnections.append(tweet)

		return tweetsConnections

mongoDBReader = MongoDBReader("TwitterCommunityDetection")
tweetsDataset = mongoDBReader.readTwitterUserTweetRetweet("new_york")

commnityDetection = CommunityDetection()
edgesDict = commnityDetection.preprocessDataset(tweetsDataset)
commnityDetection.buildGraph(edgesDict)
commnityDetection.applyLouvain()

		