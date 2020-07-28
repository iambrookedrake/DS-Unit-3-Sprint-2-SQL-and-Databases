import pandas as pd
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")


# Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

print("CONNECTION:", connection)

# A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor()
print("CURSOR:", cursor)

connection.commit()

##Create Passenger Table in PostGRES ####
create_titanic_table_query = '''
DROP TABLE IF EXISTS titanic_table;
CREATE TABLE titanic_table (
   id                      SERIAL PRIMARY KEY,
   Survived                BOOLEAN  NOT NULL,
   Pclass                  INTEGER  NOT NULL,
   Name                    VARCHAR(81) NOT NULL,
   Sex                     VARCHAR(20) NOT NULL,
   Age                     NUMERIC(3) NOT NULL,
   SiblingsSpouses_Aboard  INTEGER  NOT NULL,
   ParentsChildren_Aboard  INTEGER  NOT NULL,
   Fare                    NUMERIC(8,4) NOT NULL
)
'''

cursor.execute(create_titanic_table_query)
connection.commit()

FILEPATH = 'titanic.csv'
df = pd.read_csv(FILEPATH)
print(df.columns.tolist())
# Converts to np.bool
df["Survived"] = df["Survived"].values.astype(bool)
# Converts numpy dtypes to native python dtypes (avoids psycopg2.ProgrammingError: can't adapt type 'numpy.int64')
df = df.astype("object")

titanic_list = list(df.to_records(index=False))

insert_query = f"""INSERT INTO titanic_table (survived, pclass, name, sex, age, SiblingsSpouses_Aboard, ParentsChildren_Aboard, fare) VALUES %s"""
execute_values(cursor, str(insert_query), titanic_list)


# CLEAN UP
connection.commit() # actually save the records / run the transaction to insert rows
print('Titanic Data successfully saved to Postgres!')
