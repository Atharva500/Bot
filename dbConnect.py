import sqlite3
import json
from datetime import datetime

timeframe = "2015-01"
sql_transanction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

# Query to Create MySQL Database
def createTable():
    c.execute("""CREATE TABLE IF NOT EXISTS reddit_comments (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,
                parent TEXT,comment TEXT,subreddit TEXT, unix INT, score INT) """)

def format_data(data):
    data=data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return data

def find_parent(id):
    try:
        sql = "SELECT comment FROM reddit_comments WHERE comment_id='{}' LIMIT 1".format(id)
        c.execute(sql)
        result = c.fetchone()
        if result!=None:
            return result[0]
        else:
            return False    
    except Exception as e:
        # print("Find parent",e)
        return False

# Compute a score for each comment
def find_comment_score(id):
    try:
        sql = "SELECT score FROM reddit_comments WHERE parent_id='{}' LIMIT ".format(id)
        c.execute(sql)
        result = c.fetchone()
        if result!=None:
            return result[0]
        else:
            return False    
    except Exception as e:
        # print("Find parent",e)
        return False

# Define criteria to filter comments
def acceptable(data):
    if len(data.split(' '))>50 or len(data.split(' '))<1:
        return False
    elif len(data)>1000:
        return False
    elif data=='[deleted]' or data=='[removed]':
        return False
    else:
        return True

# Insert and replace queries
def insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql="""UPDATE reddit_comments SET parent_id=?, comment_id=?, parent=? , comment=?, subreddit=?, unix=?, score=? WHERE parent_id=?;""".format(parentid,commentid,parent,comment,subreddit,int(time),score,parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s-UPDATE insertion',str(e))    

def insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO reddit_comments (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion',str(e))


def insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO reddit_comments (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NOPARENT insertion ',str(e))

def transaction_bldr(sql):
    global sql_transanction
    sql_transanction.append(sql)
    if len(sql_transanction)>1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transanction:
            try:
                c.execute(s)
            except:
                pass
            connection.commit()
            sql_transanction = []


if __name__=='__main__':
    createTable()
    row_counter = 0
    paired_rows = 0

    with open('C:/Users/Atharva Hiwarekar/Desktop/Practice/ChatBot/ChatBot/RC_2015-01') as f:
        for row in f:
            # print(row)
            row_counter+=1
            row=json.loads(row)
            parent_id=row['parent_id']
            comment_id=row['name']
            body=format_data(row['body'])
            created_utc=row['created_utc']
            score = row['score']
            subreddit=row['subreddit']
            parent_data = find_parent(parent_id)

            if score>=2:
                if acceptable(body):
                    existing_score = find_comment_score(parent_id)
                    if existing_score:
                        if existing_score<score:
                            insert_replace_comment(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                    else:
                        if parent_data:
                            insert_has_parent(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                            paired_rows+=1
                        else:
                            insert_no_parent(comment_id,parent_id,body,subreddit,created_utc,score)
            
            if row_counter%100000==0:
                print('Total rows read: {}, Paired rows: {}, Time: {},'.format(row_counter,paired_rows,str(datetime.now())))

