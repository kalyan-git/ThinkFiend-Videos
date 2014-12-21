import urllib2
import json
import MySQLdb
import traceback
import sys

class TEDCrawler:
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

        createTEDVideos = "create table if not exists ted_videos(id int, recorded_date varchar(256), event_id varchar(256))"
        self.curr.execute(createTEDVideos)
         
        self.db.commit()

    def processTalks(self,latest=False):
        offset=0
        allVideosCrawled = False
        maxTEDId = 0
        if latest == 'False' :
            self.cleanTEDVideos()
        else:
            maxTEDId = self.getMaxTEDId()
        while not allVideosCrawled:
            if latest =='False' or maxTEDId is None:
                url = "http://api.ted.com/v1/talks.json?api-key=7zyxz2swjpwz5sabbhz8yb6y&limit=100&fields=tags&offset=" + str(offset)
            else:
                url = "http://api.ted.com/v1/talks.json?api-key=7zyxz2swjpwz5sabbhz8yb6y&limit=100&fields=tags&offset=" + str(offset) + "&filter=id:>" + str(maxTEDId)
            print 'ted url is ' + url
            tedResponse = json.loads(urllib2.urlopen(url).read())
            currentTotal = tedResponse['counts']['this']
            if currentTotal == 0:
                allVideosCrawled = True
                continue
            tedTalks = tedResponse['talks']
            for tedTalk in tedTalks:
                try:
                    self.addTalk(tedTalk)    
                except Exception:
                    traceback.print_exc(file=sys.stdout)
            self.db.commit()
            offset = offset + 100
        self.db.close()    

    def cleanTEDVideos(self):
        self.curr.execute("delete from videos where source='TED'")
        self.curr.execute("delete from video_topics where video_id in (select video_id from videos where source='TED')")
        self.curr.execute("delete from ted_videos")
        self.db.commit()

    def getMaxTEDId(self):
        self.curr.execute("select max(id) from ted_videos")
        maxId = self.curr.fetchall()
        return maxId[0][0]                        

    def addTalk(self,tedTalk):
        talk = tedTalk['talk']
        title = talk['name']
        title = self.removeUnicode(title)
        title = title.lower()
        description = talk['description']
        description = self.removeUnicode(description)
        description = description.lower()
        video_id = str(talk['id'])
        tedUrl = 'http://www.ted.com/talks' + talk['slug']
        createdDate = talk['released_at']
        updatedDate = talk['updated_at']
        print 'title is ',title
        insertString = "insert into videos(video_id,title,description,url,created_date,updated_date,source) values (\"" + video_id + '","' +  str(MySQLdb.escape_string(str(title))) + '","' + str(MySQLdb.escape_string(str(description))) + '","' + str(MySQLdb.escape_string(str(tedUrl))) +  '","' + str(MySQLdb.escape_string(str(createdDate))) + '","' + str(MySQLdb.escape_string(str(updatedDate))) + '","' + self.source + "\")"
        self.curr.execute(insertString)
        topics = talk['tags']
        for topic in topics:
            topic = topic.lower()
            insertString = "insert into video_topics(video_id,topic) values (\"" + video_id + '","' + str(MySQLdb.escape_string(str(topic))) + '")'
            self.curr.execute(insertString)
        recorded_date = talk['recorded_at']
        event_id = talk['event_id']
        insertString = "insert into ted_videos(id,recorded_date,event_id) values (\"" + str(talk['id']) + '","' + str(recorded_date) + '","' + str(event_id) + '")'
        self.curr.execute(insertString)

    def removeUnicode(self, description):
        newDescr = ""
        for ch in description:
            if ord(ch)<128:
                newDescr = newDescr + ch
        return newDescr
#ted_crawler = TEDCrawler('localhost','root','','videos_feed','TED')
#ted_crawler.processTalks()

