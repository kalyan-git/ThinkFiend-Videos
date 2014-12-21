import urllib2
import json
import MySQLdb
import traceback
import sys
import urllib

class NYTimesCrawler:
    def __init__(self, mysqlHost, mysqlUser,mysqlPassword, databaseName, source):
        self.db = MySQLdb.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword)
        self.curr = self.db.cursor()
        self.curr.execute("use " + databaseName)
        self.source = source
        self.createTablesIfNotExists()
    
    def createTablesIfNotExists(self):
        createVideos = "create table if not exists videos(video_id varchar(256),created_date varchar(256), updated_date varchar(256),title varchar(500),description text,duration varchar(100),dimension varchar(100),definition varchar(100),views int(11),likes int(11), dislikes int(11), favourites int(11), commentsCount int(11), url varchar(200), source varchar(200))"        
        self.curr.execute(createVideos)        

        createVideoTopics = "create table if not exists video_topics(video_id varchar(256), topic_id varchar(256), topic varchar(256))"
        self.curr.execute(createVideoTopics)

        self.db.commit()

    def processVideos(self,latest=False):
        latest_date = None
        if latest == 'True':
            latest_date = self.getLatestDate()
        self.curr.execute("select distinct topic from video_topics where topic is not NULL")
        for topic in self.curr.fetchall():
            if latest =='False' or latest_date is None:
                url = "http://api.nytimes.com/svc/search/v2/articlesearch.json?api-key=a8c12d2ba02c64921b3cf5765c41c742:13:69474811&fq=type_of_material:Video&q=" + urllib.quote(topic[0])
            else:
                latest_date = latest_date.replace('-','')
                url = "http://api.nytimes.com/svc/search/v2/articlesearch.json?api-key=a8c12d2ba02c64921b3cf5765c41c742:13:69474811&fq=type_of_material:Video&q=" + urllib.quote(topic[0]) + "&begin_date=" + str(latest_date)
            print 'url is ', url
            urlResponse = json.loads(urllib2.urlopen(url).read())
            if 'response' in urlResponse and 'docs' in urlResponse['response']:
                videos = urlResponse['response']['docs']
                for video in videos:
                    self.addVideo(video)
            print 'url is ' + url
        self.db.close()
     
    def getLatestDate(self):
        self.curr.execute("select max(created_date) from videos where source =\"" + self.source + '"')
        latest_date = self.curr.fetchall() 
        return latest_date[0][0]

    def addVideo(self,video):
        title = video['headline']['main']
        title = self.removeUnicode(title)
        title = title.lower()
        description = video['snippet']
        description = self.removeUnicode(description)
        description = description.lower()
        video_id = str(video['_id'])
        videoUrl = video['web_url']
        createdDate = video['pub_date'][:10]
        print 'title is ',title
        deleteString = "delete from videos where video_id='" + video_id + "'"
        print 'executing delete ',deleteString
        self.curr.execute(deleteString)
        deleteString = "delete from video_topics where video_id='" + video_id + "'"
        print 'executing delete ',deleteString
        self.curr.execute(deleteString)
        insertString = "insert into videos(video_id,title,description,url,created_date,source) values (\"" + video_id + '","' +  str(MySQLdb.escape_string(str(title))) + '","' + str(MySQLdb.escape_string(str(description))) + '","' + str(MySQLdb.escape_string(str(videoUrl))) +  '","' + str(MySQLdb.escape_string(str(createdDate))) + '","' + self.source + "\")"
        print 'executing insert'
        self.curr.execute(insertString)
        topics = video['keywords']
        for topicObj in topics:
            topic = topicObj['value']
            topic = topic.lower()
            insertString = "insert into video_topics(video_id,topic) values (\"" + video_id + '","' + str(MySQLdb.escape_string(str(topic))) + '")'
            self.curr.execute(insertString)
        self.db.commit()

    def removeUnicode(self,description):
        newDescr = ""
        for ch in description:
            if ord(ch)<128:
                newDescr = newDescr + ch
        return newDescr
#nytimes_crawler = NYTimesCrawler('localhost','root','','videos_feed','NyTimes')
#nytimes_crawler.processVideos(True)
