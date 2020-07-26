import os
import sqlite3
import pandas as pd

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..",
                           "C:/Users/iambr/Desktop/DS-Unit-3-Sprint-2-SQL-and-Databases/module1-introduction-to-sql/rpg_db.sqlite3")
# "C:/Users/iambr/Documents/sqlite/rpg_db.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR:", cursor)
print(" ")

# How many total Characters are there?
query1 = """
SELECT
  COUNT(name) as CharacterCount
FROM charactercreator_character as characters
"""
# How many of each specific subclass?
result1 = cursor.execute(query1).fetchone()
print("Total Characters: ", result1[0])

query2a = """
SELECT
  COUNT(character_ptr_id) as ClericCount
FROM charactercreator_cleric as clerics
"""
result2a = cursor.execute(query2a).fetchone()
print("Total Clerics: ", result2a[0])

query2b = """
SELECT
  COUNT(character_ptr_id) as FighterCount
FROM charactercreator_fighter as fighters
"""
result2b = cursor.execute(query2b).fetchone()
print("Total Fighters: ", result2b[0])

query2c2 = """
SELECT
  COUNT(mage_ptr_id) as NecromancerCount
FROM charactercreator_necromancer as necromancers
"""
result2c2 = cursor.execute(query2c2).fetchone()
print("Total Necromancers: ", result2c2[0])

query2c = """
SELECT
  COUNT(character_ptr_id) as MageCount
FROM charactercreator_mage as mages
"""
result2c = cursor.execute(query2c).fetchone()
print("Total Mages including Necromancers: ", result2c[0])
print("Total Mages that are NOT Necromancers: ", result2c[0]-result2c2[0])


query2e = """
SELECT
  COUNT(character_ptr_id) as ThiefCount
FROM charactercreator_thief as thieves
"""
result2e = cursor.execute(query2e).fetchone()
print("Total Thieves: ", result2e[0])
print(" ")

# How many total Items?
query3 = """
SELECT
  COUNT(item_id) as ItemCount
FROM armory_item as items
"""
result3 = cursor.execute(query3).fetchone()
print("Total Items: ", result3[0])

# How many of the Items are weapons? How many are not?
query4 = """
SELECT
  COUNT(item_ptr_id) as WeaponCount
FROM armory_weapon as weapons
"""
result4 = cursor.execute(query4).fetchone()
print("Total Weapons: ", result4[0])
print("Total Non-Weapon Items: ", result3[0]-result4[0])
print(" ")

# How many Items does each character have? (Return first 20 rows)
# How many Weapons does each character have? (Return first 20 rows)
query5 = """
SELECT
    characters.character_id,
    COUNT(items.item_id) as CharItemCount,
    COUNT(weapons.item_ptr_id) as Charweapon_count
FROM charactercreator_character characters
LEFT JOIN charactercreator_character_inventory items ON
    items.character_id = characters.character_id
LEFT JOIN armory_weapon weapons ON
    weapons.item_ptr_id == items.item_id
GROUP BY characters.character_id

"""
result5 = cursor.execute(query5).fetchall()
# Need Help here
'''I need help here:::
def get_data(query, connection):
    Function to get data from sqlite db
    cursor = connection.cursor()
    result = cursor.execute(query).fetchall()

    #Get columns from cursor object
    #columns = list(map(lambda x: x[0], cursor.description))

    # assign to DataFrame
    df = pd.DataFrame(data=result, columns=columns)
    return df

get_data(query5, connection)'''
df = pd.DataFrame(data=result5, columns=['character_id',
                  'Item_Count', 'Weapon_Count'])

print("Total Items per Character:")
print(df.head(20))
print(" ")

# On average, how many Items does each Character have?
# On average, how many Weapons does each character have?
query6 = """
SELECT
    AVG(CharItemCount) AS avg_items,
    AVG(CharWeaponCount) AS avg_weapons
FROM (
    SELECT
        characters.character_id,
        COUNT(items.item_id) as CharItemCount,
        COUNT(weapons.item_ptr_id) as CharWeaponCount
    FROM charactercreator_character characters
    LEFT JOIN charactercreator_character_inventory items ON
        items.character_id = characters.character_id
    LEFT JOIN armory_weapon weapons ON
        weapons.item_ptr_id == items.item_id
    GROUP BY characters.character_id
)
"""
result6 = cursor.execute(query6).fetchall()
print("Average Number of Items Among ALL Characters: ", round(result6[0][0],2))
print("Average Number of Weapons Among ALL Characters: ", round(result6[0][1],2))

# EXTRA ::: Item and Weapon count among ONLY those who actually have weapons
query7 = """
SELECT
    AVG(CharItemCount) AS avg_items,
    AVG(CharWeaponCount) AS avg_weapons
FROM (
    SELECT
        characters.character_id,
        COUNT(items.item_id) as CharItemCount,
        COUNT(weapons.item_ptr_id) as CharWeaponCount
    FROM charactercreator_character characters
    LEFT JOIN charactercreator_character_inventory items ON
        items.character_id = characters.character_id
    LEFT JOIN armory_weapon weapons ON
        weapons.item_ptr_id == items.item_id
    GROUP BY characters.character_id
)
WHERE CharWeaponCount > 0
"""
result7 = cursor.execute(query7).fetchall()
print("Average Items Among Weapon Holders: ", round(result7[0][0],2))
print("Average Weapons Among Weapon Holders: ", round(result7[0][1],2))
