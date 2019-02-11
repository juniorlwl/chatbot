# -*- coding: utf-8 -*-
"""
Created on Fri Aug 17 10:58:38 2018

@author: kunlelawal
"""

"""
Creating training data for our models

Format: FROM file to a TO file
tensorflow: sequence to sequence
variable length input, variable length output

Want to create a parent comment file, then a reply file where each row/line number corresponds to the other file

line 15 in parent file = line in in reply file
"""

import sqlite3
import pandas as pd

#You can have multiple timeframes
timeframes = ['2018-05']

for timeframe in timeframes:
    #Build a connection and read sql from pandas
    connection = sqlite3.connect("{}.db".format(timeframe))
    c = connection.cursor()
    #How much we're going to pull at a time to throw into our pandas dataframe
    limit = 5000
    #Allows us to buffer through out database
    #We pull. Grab last unix time stamp from pull. So on next pull, unix must be greater than last unix
    last_unix = 0
    cur_length = limit
    counter = 0
    #To see how sample(5000 rows of data) is doing
    test_done = False
    
    """
    As long as we're able to get our current limit from the database, we probably have more pulls to make. So we'll make more pulls
    """
    while cur_length == limit:
        #unix is greater than las unix which starts at 0
        df = pd.read_sql("""SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}""".format(last_unix, limit), connection)
        #Because you ordered the table in the sql statement, you can take last unix
        #df.tail(1) = the last thing
        last_unix = df.tail(1)['unix'].values[0]
        #Should be what the limit is
        cur_length = len(df)
        
        if not test_done:
            #open a file "test.from" with intention to append 'a'
            with open("test.from", 'a', encoding = 'utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            
            with open("test.to", 'a', encoding = 'utf8') as f:
                for content in df['comment'].values:
                    f.write(content+'\n')
                    
            test_done = True
        #Assuming test is done
        #Do same thing, but call the file name train.from and train.to
        else:
            #open a file "test.from" with intention to append 'a'
            with open("train.from", 'a', encoding = 'utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            
            with open("train.to", 'a', encoding = 'utf8') as f:
                for content in df['comment'].values:
                    f.write(content+'\n')
                    
        #While cur_length == limit - this is a comment. don't uncomment
        counter += 1
        #THis is for every 100,000th row completed, we're going to get the test and train data files
        if counter % 20 == 0:
            print(counter*limit, "rows completed so far")
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        