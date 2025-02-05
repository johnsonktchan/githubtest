'''

This application will read the mailbox data (mbox.txt) and 
count the number of email messages per organization (i.e. domain name of the email address) 
using a database with the following schema to maintain the counts.

"CREATE TABLE Counts (org TEXT, count INTEGER)"

The data file for this application is the same as in previous assignments: http://www.py4e.com/code3/mbox.txt.

Because the sample code is using an UPDATE statement and 
committing the results to the database as each record is read in the loop, 
it might take as long as a few minutes to process all the data. 
The commit insists on completely writing all the data to disk every time it is called.

The program can be speeded up greatly by moving the commit operation outside of the loop. 
In any database program, there is a balance between the number of operations you execute between commits 
and the importance of not losing the results of operations that have not yet been committed.
'''

import sqlite3,re

fname = input('Enter file name: ')
if (len(fname) < 1): fname = 'mbox.txt'
fh = open(fname)

#Initialize Database Connection
conn = sqlite3.connect('emaildb.sqlite')
cur = conn.cursor()

#Establish Database Schema
cur.execute('DROP TABLE IF EXISTS Counts')
cur.execute('CREATE TABLE Counts (org TEXT, count INTEGER)')

#Loop thru the data file
for line in fh:
    if not line.startswith('From: '): continue
    org = re.findall('@(.+)',line)[0]
    cur.execute('SELECT count FROM Counts WHERE org = ? ', (org,))
    row = cur.fetchone()
    if row is None:
        cur.execute('''INSERT INTO Counts (org, count)
                VALUES (?, 1)''', (org,))
    else:
        cur.execute('UPDATE Counts SET count = count + 1 WHERE org = ?',
                    (org,))

conn.commit()

sqlstr = 'SELECT * FROM Counts ORDER BY count DESC LIMIT 10'

for row in cur.execute(sqlstr):
    print(str(row[0]), row[1])

cur.close()