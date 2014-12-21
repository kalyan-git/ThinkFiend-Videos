VIDEOS FEED

This module crawls youtube,ted and nytimes videos and stores the videos information in the given mysql db. 

PREREQUISITES

    1.Running mysql server 
    2.Required database in mysql needs to be created. 
    3.No need to create any tables in the db.Module takes care of table creation 
    4.Installed MySQLdb module for python. 

RUNNING THE MODULE

    1.Compilation: sh build.sh 
    2.Execution: python run.py <properties File> 
    3.Properties File should contain details of mysql database and also other required arguments for youtube videos. 
    4.resources/config.properties can be used a reference for the same.
    5.'latest' property also needs to be specified in the properties File which is used to decide whether to crawl most recent videos or all videos

Here are the crawling details of different video sources. 

YOUTUBE

    1.Youtube videos are crawled using youtube search API. Initial queries set are picked from the seed_queries file. Location of the same should be specified in config.properties. 
    2.Related videos information of all video results of seed_queries are downloaded using youtube related videos API and this is done recursively until a depth limit is reached. This depth limit should be specified in the properties File. 
    3.If 'latest' is True, videos are downloaded only from the most recent date of already downloaded videos. 
    4.Youtube API returns only topic ID for every video. Topic crawler should be run after youtube crawler to get topic name for every topic Id using freebase API. 

TED

    1.TED videos are crawled using TED API. 
    2.If 'latest' is False, all ted videos are deleted and all TED videos are downloaded freshly. 
    3.If 'latest' is True, only most recent videos are downloaded using the max id of already downloaded videos.
    4.No query set is needed for TED. 

NYTIMES

    1.NyTimes videos are crawled using NyTimes API.
    2.Topic names of ted and youtube videos are selected as queries set for Nytimes API. 
    3.If 'latest' if True, videos are downloade only from the most recent of already downloaded videos.
