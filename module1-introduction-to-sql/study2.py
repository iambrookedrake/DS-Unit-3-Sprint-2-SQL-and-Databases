import sqlite3

connection = sqlite3.connect("Chinook_Sqlite.sqlite3")
cursor = connection.cursor()

def question6(query):

#6. Get the name of all Black Sabbath tracks and the albums they came off of
query1 = '''
SELECT AVG(age) FROM students
'''
result1 = cursor.execute(query1).fetchone()
#result1 = cursor.fetchone()
print(f'What is the average age? {result1[0]}')

if __name__ == "__main__":

        question6()


#7. What is the most popular genre by number of tracks?
#8. Find all customers that have spent over $45
#9. Find the first and last name, title, and the number of customers each employee has helped. If the customer count is 0 for an employee, it doesn't need to be displayed. Order the employees from most to least customers.
#10. Return the first and last name of each employee and who they report to