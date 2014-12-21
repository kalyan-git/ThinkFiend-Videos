import sys
from youtube_crawler import YoutubeCrawler
from get_topic_names import YoutubeTopicCrawler
from ted_crawler import TEDCrawler 
from nytimes_crawler import NYTimesCrawler
def loadProperties(propertiesFile, properties):
    with open(propertiesFile) as inputFile:
        for line in inputFile:
            line = line.strip()
            columns = line.split('=')
            if len(columns)==2:
                key = columns[0].strip()
                value = columns[1].strip()
                properties[key] = value

propertiesFile = sys.argv[1]
properties={}

loadProperties(propertiesFile,properties)

print 'Running youtube source'
youtubeCrawler = YoutubeCrawler( properties['mysqlHost'] , properties['mysqlUser'] , properties['mysqlPassword'] , properties['mysqlDatabase'],'YOUTUBE' )
youtubeCrawler.process_videos( properties['seed_queries'], properties['youtubeDepthLimit'], properties['latest'])

print 'Getting topic names for youtube topic ids'
youtubeTopicCrawler = YoutubeTopicCrawler( properties['mysqlHost'], properties['mysqlUser'], properties['mysqlPassword'], properties['mysqlDatabase'] )
youtubeTopicCrawler.get_topic_names()

print 'Running ted source'
tedCrawler = TEDCrawler( properties['mysqlHost'], properties['mysqlUser'], properties['mysqlPassword'], properties['mysqlDatabase'], 'TED')
tedCrawler.processTalks(properties['latest'])

print 'Running nytimes source'
nytimesCrawler = NYTimesCrawler( properties['mysqlHost'], properties['mysqlUser'], properties['mysqlPassword'], properties['mysqlDatabase'], 'NyTimes')
nytimesCrawler.processVideos(properties['latest'])
