import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")

'''
DB_NAME = 'vzzoxwvs'
DB_USER = 'vzzoxwvs'
DB_PASS = 'iIIvP8NBWsCVtAY8gdkgiVr_nz5SYwjl'
DB_HOST = 'hansken.db.elephantsql.com'
'''

### Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

print("CONNECTION:", connection)

### A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor()
print("CURSOR:", cursor)

connection.commit()
### An example query
#cursor.execute('SELECT * from titanic')

#results = cursor.fetchall()
#print(results)
#connection.commit()
#########

#df = pd.read_csv("titanic.csv")
#df.to_records()


##Create Character Table in PostGRES ####
create_titanic_table_query = '''
DROP TABLE IF EXISTS titanic_1;
DROP TABLE IF EXISTS titanic_list;
DROP TABLE IF EXISTS titanic_table;
CREATE TABLE titanic_table (
   Survived                BIT  NOT NULL PRIMARY KEY,
   Pclass                  INTEGER  NOT NULL,
   Name                    VARCHAR(81) NOT NULL,
   Sex                     VARCHAR(20) NOT NULL,
   Age                     NUMERIC(3) NOT NULL,
   SiblingsSpouses_Aboard  INTEGER  NOT NULL,
   ParentsChildren_Aboard  INTEGER  NOT NULL,
   Fare                    NUMERIC(8,2) NOT NULL
)
'''

cursor.execute(create_titanic_table_query)
connection.commit()

cursor.execute('SELECT * from titanic')
passengers = cursor.fetchall()
## insert data in postgres#####

for passenger in passengers:
    insert_query = f'''INSERT INTO titanic_table
		(Survived, Pclass, Name, Sex, Age, SiblingsSpouses_Aboard, ParentsChildren_Aboard, Fare) VALUES
    	{passenger}
    '''
    cursor.execute(insert_query)

results2 = cursor.fetchall()
print("results2: ", results2)
#print(insert_query)
connection.commit()

