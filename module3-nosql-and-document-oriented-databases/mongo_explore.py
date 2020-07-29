import pymongo
import os
from dotenv import load_dotenv
from pdb import set_trace as breakpoint
from pymongo import MongoClient

load_dotenv()

DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")


connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)


client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)
print("----------------")
print(client.list_database_names())

db = client.sample_analytics
print(db.list_collection_names())

#Access a specific collection
customers = db.customers
print(customers.count_documents({}))


print("------------")
print("Assignment 3:")

import json
with open('test_data_json.txt') as json_file:
    rpg_data = json.load(json_file)

my_db = client.rpg_data
character_table = my_db.characters
character_table.update(rpg_data)#query and update(containing docs)
#character_table.insert_many(rpg_data)#Upsert True or Update
print(character_table.count_documents({}))

#breakpoint()
client.close()

print("Question 1:")
print("How was working with MongoDB different from working with PostgreSQL? ")
print("I find MongoDB to be more mechanical and less intuitive than PostgreSQL.")
print("MongoDB is less wordy and with experience that could save time.")
print("Document-based databases can be useful when building databases in pieces,")
print("like if data is entered from seperate locations")
print("or over the course of several released versions.")
print(" ")
print("Question 2:")
print("What was easier, and what was harder?")
print("It's not difficult but tedious, to change bits of code here and there.")
print("PostgreSQL is closer to a spoken language that makes sense to read.")
print("I prefer sqlite for its simplicity.")
