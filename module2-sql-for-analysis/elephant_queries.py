
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")

'''
DB_NAME = 'wwcbdhmb'
DB_USER = 'wwcbdhmb'
DB_PASS = '0HSGCBfETeI4y9QmtYcD8Mm3EVurQ1US'
DB_HOST = 'hansken.db.elephantsql.com'
'''


### Connect to ElephantSQL-hosted PostgreSQL
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

### A "cursor", a structure to iterate over db records to perform queries
cursor = conn.cursor()

### An example query
cursor.execute('SELECT * from test_table;')

### Note - nothing happened yet! We need to actually *fetch* from the cursor
results = cursor.fetchall()
print(results)

'''
###Connect to 

import sqlite3

sl_conn = sqlite3.connect("rpg_db.sqlite3")
sl_cursor = sl_conn.cursor()
characters = sl_cursor.execute('SELECT * from charactercreator_character')
print(characters)

##Create Character
create_character_table_query = '''
'''
CREATE TABLE IF NOT EXISTS rpg_characters (
    	character_id SERIAL PRIMARY KEY,
	name VARCHAR(30),
	level INT,
	exp INT,
	hp INT,
	strength INT,
	intelligence INT,
	dexterity INT,
	wisdom INT
)
'''
'''
cursor.execute(create_character_table_query)
conn.commit()

## insert character data
for character in characters:
    insert_query = f''''''INSERT INTO rpg_characters (character_id, name, level, esp, hp, strength, intelligence, dexterity, wisdom)
    {character}
    '''
'''
    cursor.execute(insert_query)

print(insert_query)
'''