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

		# self.g.write_gml("fires.gml")

	def applyLouvain(self):

		partition = louvain.find_partition(self.g, louvain.ModularityVertexPartition)
		igraph.plot(partition)

		#print(partition[0])

	'''
	Transform userId - userFollowerIds in order to be fed into igraph
	Use Adjacency List 
	'''
	def preprocessDataset(self, twitterDatset):

		userFollwersDict = {}
		nodeId = 0
		edgesDict = {}

		for userInfo in twitterDatset:
			userFollwersDict[userInfo["user"]["userId"]] = userInfo["user"]["userFollowersIds"]

		users = list(set(userFollwersDict.keys()))
		edgesDict = collections.defaultdict(list)

		nodeId = 0

		for key in userFollwersDict:
			edgesDictKey = nodeId
			for u in users:
				if u in userFollwersDict[key]:
					edgesDict[edgesDictKey].append(nodeId+1)
					nodeId = nodeId + 1

		# print(len(list(edgesDict.keys())))

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

	def readTwitterDataset(self, collection, selectedFields):
		tweetsConnections = []

		tweetsConnectionsCursor = self.mongoDriver.getRecords(collection, {}, selectedFields)

		for tweet in tweetsConnectionsCursor:
			tweetsConnections.append(tweet)

		return tweetsConnections

mongoDBReader = MongoDBReader("TwitterCommunityDetection")
tweetsDataset = mongoDBReader.readTwitterDataset("australiaFires_3_1_2019_13_00", {'_id': 0, 'user.userId':1, 'user.userFollowersIds':1})

commnityDetection = CommunityDetection()
edgesDict = commnityDetection.preprocessDataset(tweetsDataset)
commnityDetection.buildGraph(edgesDict)
commnityDetection.applyLouvain()

		