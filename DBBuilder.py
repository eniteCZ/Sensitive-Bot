import json
import sqlite3
from datetime import datetime

"""The downloaded reddit repositories are very bulky and need to be stripped of unuseful data. This script transforms the JSON-formated
data into a sql database. It only inserts information which we are interested in and pairs posts and replies together. At each step
it executs a purge of long unpaired comments to reduce querrying time."""

time = ["2015-05"]
#Specify which reddit batches are to be transformed
start = 0
#When interrupting the script to make improvements set this to last batch to start where I left off
cleanup = 250000
#This sets the step at which all but the last 30k unpaird comments are purged from the db


package = []
"""Because we need to insert millions of comment-replies we want to create a buffer package and upload it to the DB every n steps.
This is more efficient than updating the DB one comment at a time"""

conn = sqlite3.connect(r"C:\Users\fradm\Desktop\School_stuff\Thesis\Code\TrainingSet.db")
c = conn.cursor()
#Establish a connection to the DB and create a cursor.

def NewTable():
    c.execute("""CREATE TABLE IF NOT EXISTS interactions
              (parent_id TEXT PRIMARY KEY, comment_id TEXT,parent TEXT, comment TEXT, subreddit TEXT, score INT)""")
#We create a table here if it does not exist yet

def cleaner(text):
    text = text.replace("\n"," StartNewLine ").replace("\r"," StartNewLine ").replace('"',"'").replace('/'," ").strip("*()[]{}")
    return text
#This function cleans the comment body of misc characters which would confuse the neural network

def find_parent(parent_id):
    try:
        sql = "SELECT comment FROM interactions WHERE comment_id = '{}' LIMIT 1".format(parent_id)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as E:
        print("find_parent failed", E)
        return False

"""This function queries the database for parent comments. When it finds one the script knows it
should pair the current comment to the parent as a response"""


def find_prev_scr(parent_id):
    try:
        sql = "SELECT score FROM interactions WHERE parent_id = '{}' LIMIT 1".format(parent_id)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as E:
        print("find_prev_score failed", E)
        
"""Because we only want to pair one coment with one response but usually a parent will have multiple, we define this function to
get the score of the current saved response if there is one already. Later this function makes it possible to only save the
highest-rated response to a comment."""

def usefulComment(comment):
    if len(comment.split(" ")) > 60 or len(comment) < 1:
        return False
    elif len(comment) > 1000:
        return False
    elif comment == "[deleted]" or comment == "[removed]" or comment == "deleted" or comment == "removed":
        return False
    else:
        return True

"""There is a number of comments which are not useful for a chatbot either because they are unnecessarily long or because 
they have been removed by the user or admin. This function allows us to detect these."""        
        
def Stacker(sql):
    global package
    package.append(sql)
    if len(package) > 400:
        """Buffer set to 400. It has to be high enough to make the process more efficient but not too high to miss out
        on pairs because the pairing function does not look into the buffer"""
        
        c.execute("BEGIN TRANSACTION")
        #Iterating manually due to different commands
        for p in package:
            try:
                c.execute(p)
            except Exception as E:
                print("Stacker problem "+ E)
        conn.commit()
        package = [] #Clear buffer

def sql_replace_response(comment_id, parent_id, parent_body, body, subreddit, score):
    try:
        sql = """UPDATE interactions SET parent_id = "{}", comment_id = "{}", parent = "{}", comment = "{}",subreddit = "{}", score = {} WHERE parent_id = "{}";""".format(parent_id,comment_id,parent_body,body,subreddit,score,parent_id)
        Stacker(sql)
    except Exception as E:
        print("replace_response failed", E)

def sql_insert_response(comment_id, parent_id, parent_body, body, subreddit, score):
    try:
        sql = """INSERT OR IGNORE INTO interactions (parent_id, comment_id, parent, comment,subreddit, score) VALUES ("{}","{}","{}","{}","{}",{});""".format(parent_id,comment_id,parent_body,body,subreddit,score)
        Stacker(sql)
    except Exception as E:
        print("insert_response failed", E)
        #pass
def sql_insert_parent(comment_id, parent_id, body, subreddit, score):
    try:
        sql = """INSERT OR IGNORE INTO interactions (parent_id, comment_id, comment, subreddit, score) VALUES ("{}","{}","{}","{}",{});""".format(parent_id,comment_id,body,subreddit,score)
        Stacker(sql)
    except Exception as E:
        print("insert_parent failed", E)
        #pass
        

if __name__ == "__main__":
    NewTable()
    c.execute("PRAGMA synchronous = OFF")
    c.execute('PRAGMA journal_mode = OFF')
    rc = 0
    pc = 0
    tc = 0
    uc = 0
    for comset in time:
        with open("D:/torrents/reddit_data/2015/RC_{}".format(comset),buffering = 1000) as file:
            for comment in file:
                tc+=1
                if tc > start:
                    comment = json.loads(comment)
                    comment_id = comment["name"]
                    parent_id = comment["parent_id"]
                    body = cleaner(comment["body"])
                    score = comment["score"]
                    subreddit = comment["subreddit"]
                    parent_body = find_parent(parent_id)
    
                    if score >= 2:
                        if usefulComment(body) == True:
                            rc +=1
                            if parent_body != False:
                                prev_rep_scr = find_prev_scr(parent_id)
                                if prev_rep_scr != False and score > prev_rep_scr:
                                    uc += 1
                                    sql_replace_response(comment_id, parent_id, parent_body, body, subreddit, score)
                                
                                elif prev_rep_scr == False:
                                    sql_insert_response(comment_id, parent_id, parent_body, body, subreddit, score)
                                    pc +=1
                            else:
                                sql_insert_parent(comment_id, parent_id, body, subreddit, score)
                if tc % 10000 == 0:
                    print("Total posts read: " + str(tc) + " Total posts added: " + str(rc) + " Paired: " + str(pc) +
                            " Updated: " + str(uc) + " At: " +str(datetime.now().time().strftime("%H:%M:%S")))
                if tc > start or pc == 0:
                        if tc % cleanup == 0:
                            print("Commencing the purge")
                            sql = """DELETE FROM interactions WHERE parent_id <= (
                                    SELECT parent_id FROM (
                                      SELECT parent_id FROM interactions WHERE parent IS NULL ORDER BY parent_id DESC LIMIT 1 OFFSET 200)
                                  )AND parent IS NULL"""
                            c.execute(sql)
                            conn.commit()
                            c.execute("VACUUM")
                            conn.commit()   
                          

