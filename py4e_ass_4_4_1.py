'''
This application will
    -read roster data in JSON format 
    -parse the file
    -produce an SQLite database that contains: 
        --User
        --Course
        --Member table 
    -populate the tables from the data file
'''
import json, sqlite3

#Read JSON data file
fname = "roster_data.json"
data = json.loads(open(fname).read())

#Establish SQLite3 connection 
conn = sqlite3.connect('roster.sqlite')
cur = conn.cursor()

#Database Initialization
sqlschema = ''' 
DROP TABLE IF EXISTS User;
DROP TABLE IF EXISTS Course;
DROP TABLE IF EXISTS Member;

CREATE TABLE User (
    id     INTEGER PRIMARY KEY,
    name   TEXT UNIQUE
);

CREATE TABLE Course (
    id     INTEGER PRIMARY KEY,
    title  TEXT UNIQUE
);

CREATE TABLE Member (
    user_id     INTEGER,
    course_id   INTEGER,
    role        INTEGER,
    PRIMARY KEY (user_id, course_id)
) 
'''
cur.executescript(sqlschema)

#Loop thru data to insert entry into database
for entry in data:

    if len(entry) != 3: continue 

    name = entry[0]
    course = entry[1]
    role = entry[2]

    cur.execute('INSERT OR IGNORE INTO User (name) VALUES (?)', (name,))
    cur.execute('SELECT id FROM User WHERE name=?',(name,))
    userId = cur.fetchone()[0]
    cur.execute('INSERT OR IGNORE INTO Course (title) VALUES (?)', (course,))
    cur.execute('SELECT id FROM Course WHERE title=?',(course,))
    course_id = cur.fetchone()[0]
    cur.execute('INSERT OR REPLACE INTO Member (user_id, course_id, role) VALUES (?,?,?)',(userId, course_id, role))

conn.commit()
