import pandas as pd
import os
import sqlite3

df = pd.read_csv("buddymove_holidayiq.csv")
# print(df)

# C:\Users\iambr\Desktop\DS-Unit-3-Sprint-2-SQL-and-Databases\
# module1-introduction-to-sql\
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..",
                           "C:/Users/iambr/Desktop/DS-Unit-3-Sprint-2-SQL-and-Databases/module1-introduction-to-sql/buddymove_holidayiq.sqlite3")
# C:/Users/iambr/Documents/sqlite/buddymove_holidayiq.sqlite3")
connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR:", cursor)
print(" ")

df.to_sql('review', connection, if_exists='replace')

# Count how many rows you have - it should be 249!
query1 = """
SELECT
  COUNT(*) as RowCount
FROM review
"""
result1 = cursor.execute(query1).fetchone()

print(" ")
print("Total Rows: ", result1[0])
print(" ")

# How many users who reviewed at least 100 Nature in the
# category also reviewed at least 100 in the Shopping category?
query2 = """
SELECT
    COUNT(*) as GoodReview
FROM (
    SELECT
        Nature
    FROM review
    WHERE Shopping > 99
)
WHERE Nature > 99
"""
result2 = cursor.execute(query2).fetchone()

print("Total Reviews Over 100 in BOTH Nature and Shopping: ", result2[0])
print(" ")

# Just Another way to do #2, same result..
'''
query2b = """
SELECT
    COUNT(*) as GoodReview
FROM review
WHERE Shopping > 99 AND Nature > 99
"""
result2b = cursor.execute(query2b).fetchall()
print("Total Med Reviews: ", result2b[0][0])
'''

# What are the average number of reviews for each category?
query3 = """
SELECT
    SUM(Sports) as SportsCount,
    SUM(Religious) as ReligiousCount,
    SUM(Nature) as NatureCount,
    SUM(Theatre) as TheatreCount,
    SUM(Shopping) as ShoppingCount,
    SUM(Picnic) as PicnicCount
FROM review
"""
result3 = cursor.execute(query3).fetchall()

print("Average Sports Review: ", round(result3[0][0]/result1[0], 2))
print("Average Religious Review: ", round(result3[0][1]/result1[0], 2))
print("Average Nature Review: ", round(result3[0][2]/result1[0], 2))
print("Average Theatre Review: ", round(result3[0][3]/result1[0], 2))
print("Average Shopping Review: ", round(result3[0][4]/result1[0], 2))
print("Average Picnic Review: ", round(result3[0][5]/result1[0], 2))
print(" ")
