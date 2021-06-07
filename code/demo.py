import sqlite3
import datetime
import db

con = sqlite3.connect("email_storage.db")
cur = con.cursor()

def searching():
  print('Searching . . . . .')
  file_path = '../input/input (39).txt'
  input = open(file_path, 'r')
  Lines = input.readlines()
  all_rows = ''
  i = 0

  for index, line in enumerate(Lines):
    i += 1
    row = "'{}'".format(line.replace("\n", ""))
    if all_rows != '': all_rows += ', '
    all_rows += str(row)
  
  sql = "Select * from temp where email in ({})".format(all_rows)
  cur.execute(sql)

  data = cur.fetchall()
  print(data[0])
  print(len(data))

def findDistinct():
  print('Distinct . . . . .')
  cur.execute("""
    Select DISTINCT email from temp
  """)

  data = cur.fetchall()
  print(len(data))

def findAll(table_name):
  print('All from => {}'.format(table_name))
  cur.execute("""
    Select * from {}
  """.format(table_name))

  data = cur.fetchall()
  print(len(data))

def union():
  sql = """SELECT email  FROM uniquee
    where email NOT IN (SELECT email FROM temp)"""
  
  cur.execute(sql)
  data = cur.fetchall()
  print(len(data))
  

if __name__ == '__main__':
  # searching()

  # findDistinct()

  findAll('unique_gmail')
  findAll('unique_other_email')
  # findAll('temp')
  findAll('duplicate')

  # union()

  # date = datetime.datetime.now()
  # file_path = '../feadback/unique_{}__T{}.txt'.format(date, len(data)).replace(" ", "_").replace(":", "-")
  # db.saveIntoFile(file_path, data)

 
  # db.getFromTable('gmail', 1, 1000)
  # db.create_temporary_tables()
  # db.processTemporaryEmail()
  print()
