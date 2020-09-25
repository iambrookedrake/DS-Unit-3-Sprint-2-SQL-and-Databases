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

FILEPATH = 'module4-acid-and-database-scalability-tradeoffs/titanic.csv'
df = pd.read_csv(FILEPATH)
#print(df.columns.tolist())
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
querytotal = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
"""
cursor.execute(querytotal)
resulttotal = cursor.fetchone()
print("Total Passengers: ", resulttotal[0])
print("-------------")
print("Assignment 4:")
print("-------------")
# QUESTION 1
print("QUESTION 1:")
print("How many passengers survived, and how many died?")
query1a = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE Survived = True
"""
cursor.execute(query1a)
result1a = cursor.fetchone()
print("Total Survived: ", result1a[0])

query1b = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE Survived = False
"""
cursor.execute(query1b)
result1b = cursor.fetchone()
print("Total Died: ", result1b[0])
print(" ")

# QUESTION 2
print("QUESTION 2:")
print("How many passengers were in each class?")
# 1st Class
query2a = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 1
"""
cursor.execute(query2a)
result2a = cursor.fetchone()
print("Total Passengers in 1st Class: ", result2a[0])

# 2nd Class
query2b = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 2
"""
cursor.execute(query2b)
result2b = cursor.fetchone()
print("Total Passengers in 2nd Class: ", result2b[0])

# 3rd Class
query2c = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 3
"""
cursor.execute(query2c)
result2c = cursor.fetchone()
print("Total Passengers in 3rd Class: ", result2c[0])
print(" ")

# QUESTION 3
print("QUESTION 3:")
print("How many passengers survived/died within each class?")
# 1st Class Survived
query3aS = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 1 AND Survived = True
"""
cursor.execute(query3aS)
result3aS = cursor.fetchone()
print("Passengers Who Survived in 1st Class: ", result3aS[0])

# 1st Class Died
query3aD = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 1 AND Survived = False
"""
cursor.execute(query3aD)
result3aD = cursor.fetchone()
print("Passengers Who Died in 1st Class: ", result3aD[0])

# 2nd Class Survived
query3bS = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 2 AND Survived = True
"""
cursor.execute(query3bS)
result3bS = cursor.fetchone()
print("Passengers Who Survived in 2nd Class: ", result3bS[0])

# 2nd Class Died
query3bD = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 2 AND Survived = False
"""
cursor.execute(query3bD)
result3bD = cursor.fetchone()
print("Passengers Who Died in 2nd Class: ", result3bD[0])

# 3rd Class Survived
query3cS = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 3 AND Survived = True
"""
cursor.execute(query3cS)
result3cS = cursor.fetchone()
print("Passengers Who Survived in 3rd Class: ", result3cS[0])

# 3rd Class Died
query3cD = """
SELECT
  COUNT(Survived)
FROM titanic_table as Survived
WHERE pclass = 3 AND Survived = False
"""
cursor.execute(query3cD)
result3cD = cursor.fetchone()
print("Passengers Who Died in 3rd Class: ", result3cD[0])
print(" ")

# QUESTION 4
print("QUESTION 4:")
print("What was the average age of survivors vs nonsurvivors?")
# Average Age Survived
query4S = """
SELECT
  SUM(age)
FROM titanic_table as Survived
WHERE Survived = True
"""
cursor.execute(query4S)
result4S = cursor.fetchone()
print("Average Age of Survivors: ", round(result4S[0]/result1a[0],1))

# Average Age Nonsurvivors
query4D = """
SELECT
  SUM(Age)
FROM titanic_table as Survived
WHERE Survived = False
"""
cursor.execute(query4D)
result4D = cursor.fetchone()
print("Average Age of NON-Survivors: ", round(result4D[0]/result1b[0],1))
print(" ")

# QUESTION 5
print("QUESTION 5")
print("What was the average age of each passenger class?")
# Average Age 1st Class
query5a = """
SELECT
  SUM(age)
FROM titanic_table as Survived
WHERE pclass = 1
"""
cursor.execute(query5a)
result5a = cursor.fetchone()
print("Average Age of 1st Class Passengers: ", round(result5a[0]/result2a[0],1))

# Average Age 2nd Class
query5b = """
SELECT
  SUM(age)
FROM titanic_table as Survived
WHERE pclass = 2
"""
cursor.execute(query5b)
result5b = cursor.fetchone()
print("Average Age of 2nd Class Passengers: ", round(result5b[0]/result2b[0],1))

# Average Age 3rd Class
query5c = """
SELECT
  SUM(age)
FROM titanic_table as Survived
WHERE pclass = 3
"""
cursor.execute(query5c)
result5c = cursor.fetchone()
print("Average Age of 3rd Class Passengers: ", round(result5c[0]/result2c[0],1))
print(" ")

# QUESTION 6
print("QUESTION 6:")
print("What was the average fare by passenger class? By survival?")
# Average Fare 1st Class
query6a = """
SELECT
  SUM(fare)
FROM titanic_table as Survived
WHERE pclass = 1
"""
cursor.execute(query6a)
result6a = cursor.fetchone()
print("Average Fare of 1st Class Passengers: ", round(result6a[0]/result2a[0],1))

# Average Fare 2nd Class
query6b = """
SELECT
  SUM(fare)
FROM titanic_table as Survived
WHERE pclass = 2
"""
cursor.execute(query6b)
result6b = cursor.fetchone()
print("Average Fare of 2nd Class Passengers: ", round(result6b[0]/result2b[0],1))

# Average Fare 3rd Class
query6c = """
SELECT
  SUM(fare)
FROM titanic_table as Survived
WHERE pclass = 3
"""
cursor.execute(query6c)
result6c = cursor.fetchone()
print("Average Fare of 3rd Class Passengers: ", round(result6c[0]/result2c[0],1))

# Average Fare Among Survivors
query6S = """
SELECT
  SUM(fare)
FROM titanic_table as Survived
WHERE Survived = True
"""
cursor.execute(query6S)
result6S = cursor.fetchone()
print("Average Fare Among Survivors: ", round(result6S[0]/result1a[0],1))

# Average Fare Among Non-Survivors
query6D = """
SELECT
  SUM(fare)
FROM titanic_table as Survived
WHERE Survived = False
"""
cursor.execute(query6D)
result6D = cursor.fetchone()
print("Average Fare Among Non-Survivors: ", round(result6D[0]/result1b[0],1))
print(" ")

# QUESTION 7
print("QUESTION 7:")
print("How many siblings/spouses aboard on average, by passenger class?")
# Average siblingsspouses_aboard 1st Class
query7a = """
SELECT
  SUM(siblingsspouses_aboard)
FROM titanic_table as Survived
WHERE pclass = 1
"""
cursor.execute(query7a)
result7a = cursor.fetchone()
print("Average siblingsspouses_aboard of 1st Class Passengers: ", round(result7a[0]/result2a[0],2))

# Average siblingsspouses_aboard 2nd Class
query7b = """
SELECT
  SUM(siblingsspouses_aboard)
FROM titanic_table as Survived
WHERE pclass = 2
"""
cursor.execute(query7b)
result7b = cursor.fetchone()
print("Average siblingsspouses_aboard of 2nd Class Passengers: ", round(result7b[0]/result2b[0],2))

# Average siblingsspouses_aboard 3rd Class
query7c = """
SELECT
  SUM(siblingsspouses_aboard)
FROM titanic_table as Survived
WHERE pclass = 3
"""
cursor.execute(query7c)
result7c = cursor.fetchone()
print("Average siblingsspouses_aboard of 3rd Class Passengers: ", round(result7c[0]/result2c[0],2))
print(" ")

print("By survival?")
# Average siblingsspouses_aboard Among Survivors
query7S = """
SELECT
  SUM(siblingsspouses_aboard)
FROM titanic_table as Survived
WHERE Survived = True
"""
cursor.execute(query7S)
result7S = cursor.fetchone()
print("Average siblingsspouses_aboard Among Survivors: ", round(result7S[0]/result1a[0],2))

# Average siblingsspouses_aboard Among Non-Survivors
query7D = """
SELECT
  SUM(siblingsspouses_aboard)
FROM titanic_table as Survived
WHERE Survived = False
"""
cursor.execute(query7D)
result7D = cursor.fetchone()
print("Average siblingsspouses_aboard Among Non-Survivors: ", round(result7D[0]/result1b[0],2))
print(" ")


# QUESTION 8
print("QUESTION 8:")
print("How many parents/children aboard on average, by passenger class?")
# Average parentschildren_aboard 1st Class
query8a = """
SELECT
  SUM(parentschildren_aboard)
FROM titanic_table as Survived
WHERE pclass = 1
"""
cursor.execute(query8a)
result8a = cursor.fetchone()
print("Average parentschildren_aboard of 1st Class Passengers: ", round(result8a[0]/result2a[0],2))

# Average parentschildren_aboard 2nd Class
query8b = """
SELECT
  SUM(parentschildren_aboard)
FROM titanic_table as Survived
WHERE pclass = 2
"""
cursor.execute(query8b)
result8b = cursor.fetchone()
print("Average parentschildren_aboard of 2nd Class Passengers: ", round(result8b[0]/result2b[0],2))

# Average parentschildren_aboard 3rd Class
query8c = """
SELECT
  SUM(parentschildren_aboard)
FROM titanic_table as Survived
WHERE pclass = 3
"""
cursor.execute(query8c)
result8c = cursor.fetchone()
print("Average parentschildren_aboard of 3rd Class Passengers: ", round(result8c[0]/result2c[0],2))
print(" ")

print("By survival?")
# Average parentschildren_aboard Among Survivors
query8S = """
SELECT
  SUM(parentschildren_aboard)
FROM titanic_table as Survived
WHERE Survived = True
"""
cursor.execute(query8S)
result8S = cursor.fetchone()
print("Average parentschildren_aboard Among Survivors: ", round(result8S[0]/result1a[0],2))

# Average parentschildren_aboard Among Non-Survivors
query8D = """
SELECT
  SUM(parentschildren_aboard)
FROM titanic_table as Survived
WHERE Survived = False
"""
cursor.execute(query8D)
result8D = cursor.fetchone()
print("Average parentschildren_aboard Among Non-Survivors: ", round(result8D[0]/result1b[0],2))
print(" ")

# QUESTION 9
print("QUESTION 9:")
# Do any passengers have the same name?
query9 = """
SELECT
    COUNT(name)
FROM 
	(
		SELECT DISTINCT name FROM titanic_table
	) AS UniqueNames
"""
cursor.execute(query9)
result9 = cursor.fetchone()

if result9[0]-resulttotal[0]==0:
  samename='No'
else:
  samename='Yes'
print("Do any passengers have the same name? ",samename)

print("Passengers with Same Name: ", result9[0]-resulttotal[0])
print(" ")

connection.commit()
