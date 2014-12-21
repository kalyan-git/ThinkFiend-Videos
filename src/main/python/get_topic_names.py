import MySQLdb
import urllib2
import json
import urllib
from youtube_crawler import SSLAdapter
import requests

class YoutubeTopicCrawler:

    def __init__(self, mysqlHost, mysqlUser,mysqlPassword, databaseName):
        self.db = MySQLdb.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword)
        self.SSLSession = requests.Session()
        self.SSLSession.mount('https://', SSLAdapter())
        self.curr = self.db.cursor()
        self.curr.execute("use " + databaseName)
        self.createTablesIfNotExists()
        self.curr.execute("select distinct topic_id from video_topics where topic is NULL")

    def createTablesIfNotExists(self):
        createVideoTopics = "create table if not exists video_topics(video_id varchar(256), topic_id varchar(256), topic varchar(256))"
        self.curr.execute(createVideoTopics)

        self.db.commit()

    def get_topic_names(self):
        baseUrl = "https://www.googleapis.com/freebase/v1/mqlread?autorun=true&key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&query="
        query_json={}
        query_json["id"]=None
        query_json["name"]=None
        query_json["id|="]=[]
        query=[query_json]
        count = 0
        videos_topics={}
        for topic_id in self.curr.fetchall():
            count = count + 1
            query_json["id|="].append(topic_id[0])
            if count%2==0 and len(query_json["id|="])>0:
                freebaseUrl = baseUrl + urllib.quote(json.dumps(query))
                print 'freebaseUrl is ',freebaseUrl
                freebaseResponse = json.loads(self.getSSLUrlContent(freebaseUrl))
                result = freebaseResponse['result']
                for topicJSON in result:
                    topic_id = topicJSON["id"]
                    topic_name = topicJSON["name"]
                    if topic_id and topic_name:
                        try:
                            insertString = "update video_topics set topic=" + '"' + MySQLdb.escape_string(topic_name) + '"' +  " where topic_id=\"" + topic_id + '"'
                            print 'insertString is ',insertString
                            self.curr.execute(insertString)
                        except Exception:
                            continue
                self.db.commit()
                query_json["id|="] = []
                videos_topics = {}
        self.db.close()
    
    def getSSLUrlContent(self, url) :
        urlResponse = self.SSLSession.get(url)
        return urlResponse.text
#youtube_topic_crawler = YoutubeTopicCrawler('localhost','root','','videos_feed')
#youtube_topic_crawler.get_topic_names()
