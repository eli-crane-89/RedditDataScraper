import praw
from praw.models import MoreComments
import pandas as pandas
import datetime as dt
import pymongo
import time
import configparser



class RedditDataScraperController(object):

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("properties_local.ini")
        self.reddit = praw.Reddit(client_id=config['Credentials']['ClientID'],
             client_secret=config['Credentials']['ClientSecret'],
             user_agent=config['Credentials']['UserAgent'],
             username=config['Credentials']['UserName'],
             password=config['Credentials']['Password'])
        self.client = pymongo.MongoClient(config['Database']['Server'])
        self.db = self.client[config['Database']['Instance']]

        self.submissionLimit = 100
        self.submissionCount = 0
        self.commentLimit = 500
        self.commentCount = 0
        self.timeLimitCreated = 5*60
        self.timeLimitThread = 10*60*60
        self.timeLimitComment = 10*60*60
        self.timeLimitCommentSearch = 600
        self.period = 300
        self.dictComments = {}
    
    def runProcess(self):
        
        tpSubreddits = ('news','worldnews','philosophy','futurology','twoxchromosomes','science','music','movies','technology')

        dictActiveThreads = {}
        for sub in tpSubreddits:
            dictActiveThreads[sub] = {}


        start = time.time()
        i=0

        while (i < 500):
            iterationStart = time.time()
            for sub in tpSubreddits:
                print(sub)
                dictActiveThreads[sub] = self.searchBySubreddit(sub,dictActiveThreads[sub])
                print(time.time()-start)
                i+=1

            while time.time() - iterationStart < self.period:
                time.sleep(5)


    def returnNamedComments(self,sub,threadId):
        collection = self.db[sub]

        query = {"id":threadId}
        listDocs = list(collection.find(query))

        lsNamedComments = []
        if len(listDocs) > 0:
            threadDoc = listDocs[0]

            for commentKey in threadDoc['comments'].keys():
                if 'author_id' in threadDoc['comments'][commentKey].keys():
                    lsNamedComments.append(commentKey)

        return lsNamedComments


    def searchBySubreddit(self,sub,dictActiveThreads,postTime='day'):
        dictThread  = {}
        lsActiveThreads = []

        self.submissionCount = 0
        for top in self.reddit.subreddit(sub).new():
            if (time.time() - top.created_utc < self.timeLimitThread #only tracks for 10 hours
                #only articles that are being tracked or were created 5 minutes ago are captured:
                and (top.id in dictActiveThreads.keys() or time.time() - top.created_utc < self.timeLimitCreated)):
                self.submissionCount += 1
            if self.submissionCount < self.submissionLimit:
                print(top.id)
                lsActiveThreads.append(top.id)
            
            blFirstTime = False
            if top.id not in dictActiveThreads.keys():
                dictActiveThreads[top.id] = {"timeThread":time.time(),"timeComment" : time.time()}
                blFirstTime = True
            
            if blFirstTime or time.time() - dictActiveThreads[top.id]["timeThread"] > 300:
                if blFirstTime:
                    dictSubmission = {
                        "title" : top.title,
                        "score" : top.score,
                        "author_id" : top.author.id,
                        "upvote_ratio" : top.upvote_ratio,
                        "id" : top.id,
                        "url" : top.url,
                        "comms_num" : top.num_comments,
                        "created" : top.created_utc,
                        "body"  : top.selftext
                        }
                else:
                    dictSubmission = {
                    "title" : top.title,
                    "score" : top.score,
                    "upvote_ratio" : top.upvote_ratio,
                    "id" : top.id,
                    "url" : top.url,
                    "comms_num" : top.num_comments,
                    "created" : top.created_utc,
                    "body"  : top.selftext
                    }
            
            if (blFirstTime
                or not (time.time()-dictSubmission["created"] > self.timeLimitComment and time.time() - dictActiveThreads[top.id]["timeComment"] < self.timeLimitCommentSearch)):

                self.dictComments = {}
                lsNamedComments = self.returnNamedComments(sub,dictSubmission["id"])
                self.commentCount = 0
                self.traverseComments(top.comments,lsNamedComments)
                
                dictSubmission["comments"] = self.dictComments
                dictThread[dictSubmission["id"]] = dictSubmission


        self.insertRecordIntoMongo(sub,dictThread)


        dictActiveReturn = {}
        for key in dictActiveThreads.keys():
            if key in lsActiveThreads:
                dictActiveReturn[key] = dictActiveThreads[key]

        return dictActiveReturn
    
    def traverseComments(self,comments,lsNamedComments,isTopLevel=True):
        self.commentCount += 1
        if type(comments) is praw.models.reddit.comment.Comment:
            return
        else:
            for topLevelComment in comments:
                if self.commentCount <= self.commentLimit and not isinstance(topLevelComment,MoreComments):
                    if len(topLevelComment.replies) > 0:
                        self.traverseComments(topLevelComment.replies,lsNamedComments,False)
                    if topLevelComment.id in lsNamedComments or not isTopLevel:
                        self.dictComments[topLevelComment.id] = {"body":topLevelComment.body,
                            "parent_id":topLevelComment.parent_id,
                            "score":topLevelComment.score,
                            "edited":topLevelComment.edited}
                else:
                    author = topLevelComment.author
                    if author:
                        self.dictComments[topLevelComment.id] = {"body":topLevelComment.body,
                        "parent_id":topLevelComment.parent_id,
                        "author_id":author.id,
                        "score":topLevelComment.score,
                        "edited":topLevelComment.edited
                        }





    def insertRecordIntoMongo(self,sub,dictRecords):
        collection = self.db[sub]
        
        for key in dictRecords.keys():
            dictRecords[key]['time_inserted'] = time.time()
            doc = collection.insert_one(dictRecords[key])

controller = RedditDataScraperController()
controller.runProcess()



