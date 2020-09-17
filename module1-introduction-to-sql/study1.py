import sqlite3

connection = sqlite3.connect("study_part1.sqlite3")
cursor = connection.cursor()


cursor.execute("DROP TABLE IF EXISTS students")

create_table_query = '''CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student VARCHAR(80),
    studied TEXT,
    grade INT,
    age INT,
    sex TEXT)
'''
cursor.execute(create_table_query)

sample_data = [
    ('Lion-O', 'True', 85, 24, 'Male'),
    ('Cheetara', 'True', 95, 22, 'Female'),
    ('Mumm-Ra', 'False', 65, 153, 'Male'),
    ('Snarf', 'False', 70, 15, 'Male'),
    ('Panthro', 'True', 80, 30, 'Male')
]

for thundercat in sample_data:
    insert_data_query = f'''
    INSERT INTO students
    (student, studied, grade, age, sex)
    VALUES {thundercat}
    '''
    #print(insert_data_query)
    cursor.execute(insert_data_query)

connection.commit()


#What is the average age? Expected Result - 48.8
query1 = '''
SELECT AVG(age) FROM students
'''
result1 = cursor.execute(query1).fetchone()
#result1 = cursor.fetchone()
print(f'What is the average age? {result1[0]}')



# What are the name of the female students? Expected Result - 'Cheetara'
# How many students studied? Expected Results - 3
# Return all students and all columns, sorted by student names in alphabetical order. 
