import urllib
import json
import MySQLdb
import re
import traceback
import sys
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
import ssl
import requests

class SSLAdapter(HTTPAdapter):
    def init_poolmanager (self,connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_version=ssl.PROTOCOL_TLSv1)

class YoutubeCrawler: 
    def __init__(self, mysqlHost, mysqlUser,mysqlPassword, databaseName, source):
        self.db = MySQLdb.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword)
        self.curr = self.db.cursor()
        self.curr.execute("use " + databaseName)
        self.source = source
        self.SSLSession = requests.Session()
        self.SSLSession.mount('https://', SSLAdapter())
        self.videosProcessed={}
        self.depthLimit = -1
        self.createTablesIfNotExists()

    def createTablesIfNotExists(self):
        createVideos = "create table if not exists videos(video_id varchar(256),created_date varchar(256), updated_date varchar(256),title varchar(500),description text,duration varchar(100),dimension varchar(100),definition varchar(100),views int(11),likes int(11), dislikes int(11), favourites int(11), commentsCount int(11), url varchar(200), source varchar(200))"        
        self.curr.execute(createVideos)        
        
        createVideoTopics = "create table if not exists video_topics(video_id varchar(256), topic_id varchar(256), topic varchar(256))"
        self.curr.execute(createVideoTopics)

        createYoutubeVideos = "create table if not exists youtube_videos(video_id varchar(256), thumbnails varchar(10000), channelId varchar(256), channelTitle varchar(500))"
        self.curr.execute(createYoutubeVideos)

        self.db.commit()
        
    def process_videos(self,seedFile, depthLimit, latest=False):
        self.depthLimit = depthLimit
        self.curr.execute("select video_id from videos where source=\"" + self.source + '"')

        for video_id in self.curr.fetchall():
            video_id = video_id[0]
            self.videosProcessed[video_id]=1

        latest_date=None 
        if latest == 'True' : 
            latest_date = self.getLatestDateTimeStamp()
        with open(seedFile) as inputFile:
            for line in inputFile:
                seed = line.strip()
                self.crawlSeed(urllib.quote(seed),latest_date)

        self.db.close() 
    
    def getLatestDateTimeStamp(self):
        self.curr.execute("select max(created_date) from videos where source=\"" + self.source + '"')
        latest_date = self.curr.fetchall()
        return latest_date[0][0]

    def crawlSeed(self, seed, latest_date):
        if not latest_date :    
            youtubeUrl = "https://www.googleapis.com/youtube/v3/search?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=snippet,id&type=video&maxResults=50&q=" + seed
        else:
            latest_date=latest_date + 'T00:00:00.000Z'
            youtubeUrl = "https://www.googleapis.com/youtube/v3/search?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=snippet,id&type=video&maxResults=50&q=" + seed + "&publishedAfter=" + latest_date 
        print 'youtube search url is ', youtubeUrl
        try:
            youtubeResponse = self.getSSLUrlContent(youtubeUrl)
        except Exception:
            traceback.print_exc(file=sys.stdout)
            return
        jsonResponse = json.loads(youtubeResponse)
        videos = jsonResponse['items']
        videoDetails = {}
        for video in videos:
            id = video['id']['videoId']
            snippet = video['snippet']
            published_date = snippet['publishedAt'][:10]
            title = snippet['title']
            description = snippet.get('description','')
            thumbnails = json.dumps(snippet.get('thumbnails',{}))
            channelId = snippet.get('channelId','')
            channelTitle = snippet.get('channelTitle','')
            videoDetails[id]=[published_date,title,description,thumbnails,channelId,channelTitle]
        self.addVideoDetails(videoDetails)    
        for id in videoDetails:
            self.getRelatedVideos(id,1)

    def getSSLUrlContent(self, url) :
        urlResponse = self.SSLSession.get(url)
        return urlResponse.text

    def addVideoDetails(self, videoDetails):
        youtubeUrl = "https://www.googleapis.com/youtube/v3/videos?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=contentDetails,statistics,topicDetails&id="
        for id in videoDetails:
            youtubeUrl = youtubeUrl + id + ","
        youtubeUrl = youtubeUrl.strip(",")
        print 'youtube video url is ', youtubeUrl
        try:
            youtubeResponse = self.getSSLUrlContent(youtubeUrl)
        except Exception:
            traceback.print_exc(file=sys.stdout)
            return
        jsonResponse = json.loads(youtubeResponse)
        videos = jsonResponse['items']
        for video in videos:
            id = video['id']
            if id in self.videosProcessed:
                continue
            contentDetails = video.get('contentDetails',{})
            duration = contentDetails.get('duration','')
            dimension = contentDetails.get('dimension','')
            definition = contentDetails.get('definition','')
            statistics = video.get('statistics',{})
            views = int(statistics.get('viewCount',0))
            likes = int(statistics.get('likeCount',0))
            dislikes = int(statistics.get('dislikeCount',0))
            favourites = int(statistics.get('favoriteCount',0))
            commentsCount = int(statistics.get('commentCount',0))
            topicDetails = video.get('topicDetails',{})
            topicIds = topicDetails.get('topicIds',[])
            sourceSpecificDetails = videoDetails[id][-3:]
            videoDetails[id] = videoDetails[id][:-3]
            videoDetails[id].append(duration)
            videoDetails[id].append(dimension)
            videoDetails[id].append(definition)
            videoDetails[id].append(views)
            videoDetails[id].append(likes)
            videoDetails[id].append(dislikes)
            videoDetails[id].append(favourites)
            videoDetails[id].append(commentsCount)
            self.videosProcessed[id]=1
            url = "http://www.youtube.com/watch?v="+id
            insertString = "insert into videos(video_id,created_date,title,description,duration,dimension,definition,views,likes,dislikes,favourites,commentsCount,url,source) values("
            insertString  = insertString + '"' + id + '"' + ","
            try:
                for attr in videoDetails[id]:
                    insertString = insertString + '"' + str(MySQLdb.escape_string(str(attr))) + '"' +  ","
            except Exception:
                continue
            insertString = insertString + '"' + MySQLdb.escape_string(url) + '","' +  self.source + '"'
            insertString = insertString + ")"
            print 'insertString is ' + insertString
            self.curr.execute(insertString)
            insertString = "insert into youtube_videos(video_id,thumbnails,channelId,channelTitle) values("
            insertString  = insertString + '"' + id + '"' + ","
            try:
                for attr in sourceSpecificDetails:
                    insertString = insertString + '"' + str(MySQLdb.escape_string(str(attr))) + '"' +  ","
            except Exception:
                continue
            insertString = insertString.strip(",")
            insertString = insertString + ")"
            print 'insert string is ',insertString
            self.curr.execute(insertString)
            for topic_id in topicIds:
                insertString = "insert into video_topics(video_id,topic_id) values(\"" + id + "\",\""  + topic_id + "\")"
            print 'topics insert string is ' + insertString
            self.curr.execute(insertString)
        self.db.commit()   

    def getRelatedVideos(self, id, depth):
        if depth > self.depthLimit:
            return
        youtubeUrl = "https://www.googleapis.com/youtube/v3/search?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=snippet&type=video&maxResults=50&&relatedToVideoId="+id
        print 'youtube related video url is ', youtubeUrl
        try:
            youtubeResponse = self.getSSLUrlContent(youtubeUrl)
        except Exception:
            traceback.print_exc(file=sys.stdout)
            return
        jsonResponse = json.loads(youtubeResponse)
        videos = jsonResponse['items']
        videoDetails = {}
        for video in videos:
            id = video['id']['videoId']
            if id in self.videosProcessed:
                continue
            snippet = video['snippet']
            published_date = snippet['publishedAt'][:10]
            title = snippet['title']
            description = snippet.get('description','')
            thumbnails = json.dumps(snippet.get('thumbnails',{}))
            channelId = snippet.get('channelId','')
            channelTitle = snippet.get('channelTitle','')
            videoDetails[id]=[published_date,title,description,thumbnails,channelId,channelTitle]
        self.addVideoDetails(videoDetails)    
        for id in videoDetails:
            self.getRelatedVideos(id,depth+1)
#youtube_crawler = YoutubeCrawler('localhost','root','','videos_feed','YOUTUBE')
#youtube_crawler.process_videos('/Users/ssdeepa/misc/videos/seed_queries.txt',1,True)
