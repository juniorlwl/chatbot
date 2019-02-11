# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 14:56:33 2018

@author: kunlelawal
"""

import sqlite3
import json
from datetime import datetime

#format
timeframe = '2018-05'
#build up big transaction and do it all at once - FAST!
sql_transaction = []

#connect to the database
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

#Creates a table if it doesn't exist
def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)""")
    
#format data function used in the main
#make takenization of individual entities
#creates individual entities for return characters
#make all doouble quotes single quotes
def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data

#find parent by parent_id function
#for every comment we also want the parent body (text)
def find_parent(pid):
    try:
        sql = """SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1""".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: 
            return False
    except Exception as e:
        #print("find_parent", e)
        return False
    
#find existing score function
def find_existing_score(pid):
    try:
        sql = """"SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1""".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: 
            return False
    except Exception as e:
        #print("find_parent", e)
        return False
    
#Check if the data you are updating the score for is acceptable
def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True

#Transaction builder
def transaction_bldr(sql):
    #We have a bunch of things in sql_transaction, but we want to clear it out eventually
    global sql_transaction
    #We build this transaction till it's of a certain size
    sql_transaction.append(sql)
    if len(sql_transaction) >= 1000:
        c.execute("BEGIN TRANSACTION")
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        #When done, commit the transaction
        connection.commit()
        #Empty out the sql_transaction
        sql_transaction = []
        
    
def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score):
    
    """
    This overwrites the information already in the database where parent id is what the comment parent id was. -- Any reply to the parent comment is the new reply in the database IF IT HAS A BETTER SCORE 
    """
    
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id = ?;""".format(parentid, commentid, parent, comment, time, score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-UPDATE insertion', str(e))

def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    
    """
    Inserting information about the parent body
    Inserting row where we have the parent id as well as data
    """
    
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}");""".format(parentid, commentid, parent, comment, subreddit, time, score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion', str(e))
        
def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    
    """
    No parent, but we want the parent id
    """
    
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}");""".format(parentid, commentid, comment, subreddit, time,score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT insertion', str(e))

# Main code to iterate through reddit file and clean up data
if __name__ == "__main__":
    create_table()
    #tells how many rows have we gone through
    row_counter = 0
    #how many parent - child pairs. Come comment do not have replies
    paired_rows = 0
    
    #open the reddit data file and name it f
    with open("/Users/kunlelawal/Desktop/Masters/MSA/PROJECTS/Chatbot/reddit_data/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
        #start iterating through the data
        for row in f:
            #can comment out below to make faster
            #print(row)
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            comment_id = row['id']
            parent_data = row['author_flair_text']
            
            
            if score >= 2:
                #check if acceptable body first
                if acceptable(body):
                    #update the score for an existing comment if it has a better score
                    #search for existing score by parent_id
                    existing_comment_score =find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            #update row instead of insert
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            
                    #if no existing comment score
                    else:
                        #if there is a parent we have data for
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                       
            #For every 100,000th row
            if row_counter % 100000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))
                            
                        
                    
                
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        