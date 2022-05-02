import sqlite3
import datetime
import math

con = sqlite3.connect("email_storage.db")
cur = con.cursor()

def createTable(table_names):
  for i in table_names:
    Unique = ''
    if i == 'gmail' or i == 'other_email': Unique = 'UNIQUE'
    sql =  """
      CREATE TABLE IF NOT EXISTS {} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email text NOT NULL {}
      );
    """.format(i, Unique)
    cur.execute(sql)

def dropTable(table_names):
  for i in table_names:
    sql = 'DROP TABLE IF EXISTS {}'.format(i)
    cur.execute(sql)

def renameTable(from_table, to_table):
  sql = 'ALTER TABLE {} RENAME TO {}'.format(from_table, to_table)
  cur.execute(sql)

def create_temporary_tables():
  # Dropping Tables { IF EXISTS } 
  print('{ IF EXISTS } Dropping Tables . . . . .') 
  dropTable(['temp', 'duplicate', 'temp_gmail', 'temp_other_email'])

  # Creating Tables
  print('Creating Tables . . . . .') 
  createTable(['gmail', 'other_email', 'temp', 'duplicate', 'temp_other_email'])


# [START] => Process Input
def processTemporaryEmail():
  print('[START] => Processing Starts . . . . .')
  processInvalidEmail()
  seperateEmail()
  InsertIntoMainTable('temp_gmail', 'gmail')
  InsertIntoMainTable('temp_other_email', 'other_email')

  
  # Save Duplicate and Unique Emails to file
  copyAllFromTableToFile('temp_gmail', 'unique_gmail')
  copyAllFromTableToFile('temp_other_email', 'unique_other_email')
  copyAllFromTableToFile('duplicate', False)
  print('[END] => Processing Ends . . . . .')


def processInvalidEmail():
  print('Invalid Email Processing . . . . .')
  cur.execute("Select * from temp where email not Like '%.com'")
  data1 = cur.fetchall()

  cur.execute("Select * from temp where email Like '%..%'")
  data2 = cur.fetchall()

  cur.execute("Select * from temp where email NOT Like '%@%'")
  data3 = cur.fetchall()

  data = data1 + data2 + data3

  date = datetime.datetime.now()
  file_path = './output/{}_{}__T{}.txt'.format('invalid', date, len(data)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, data)
  deleteChunkFromTable('temp', data)


def seperateEmail ():
  print('Email Seperating . . . . .')
  sql = "Select * from temp where email not Like '%@gmail.com%';"
  cur.execute(sql)
  result = cur.fetchall()
  InsertFromTableToTable('temp', 'temp_other_email', result, True)
  renameTable('temp', 'temp_gmail')


def InsertIntoMainTable(from_table, to_table):
  print('Inserting into {} . . . . .'.format(to_table) )
  print('    0% => Inserting into {} . . . . .'.format(to_table) )
  result = getAllFromTable(from_table)
  duplicate = []
  p = 0 
  for index, row in enumerate(result):
    if index >= (len(result) / 100) * p:
      p += 1
      print('    {}% => Inserting into {}. . . . .'.format(p, to_table) )
    try: insertIntoTable( to_table, str("(NULL, '{}')".format(row[1])) )
    except: 
      duplicate.append(row)
    if len(duplicate) == 30000 or index == len(result) - 1:
      InsertFromTableToTable(from_table, 'duplicate', duplicate, True)
      duplicate = []

# [END] => Process Input


def InsertFromTableToTable (from_table, to_table, result, delete_flag):
  rows = ""
  ids = ""
  i = 0
  for index, row in enumerate(result):
    i += 1
    if rows != '':
      rows += ', '
      ids += ', '
    rows += str("(NULL, '{}')".format(row[1]))
    ids += str(row[0])
  
    if i == 35000 or index == len(result) - 1:
      insertIntoTable(to_table, rows)
      if delete_flag:
        sql = "DELETE FROM {} WHERE id IN ({})".format(from_table, ids)
        cur.execute(sql)
        con.commit()
      rows = ""
      ids = ""
      i = 0


def insertIntoTable(table_name, rows):
  if rows != "":
    sql = "INSERT INTO {} VALUES {}".format(table_name, rows)
    cur.execute(sql)
    con.commit()


def getChunkFromTable(table_name, start, limit):
  sql = "SELECT * FROM {} WHERE id >= {} LIMIT {}".format(table_name, start, limit)
  cur.execute(sql)
  result = cur.fetchall()

  date = datetime.datetime.now()
  file_path = './output/{}_{}__S{}-E{}_T{}.txt'.format(table_name, date, start, start+len(result)-1, len(result)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, result)


def deleteChunkFromTable(table_name, result):
  ids = ''
  i = 0

  for index, row in enumerate(result):
    i += 1
    ids += str(row[0])
    
    if i == 50000 or index == len(result) - 1:
      if ids != '':
        sql = "DELETE FROM {} WHERE id IN ({})".format(table_name, ids)
        cur.execute(sql)
        con.commit()
      ids = ''
      i = 0
    else: ids += ', '


def copyAllFromTableToFile(table_name, modified_table_name):
  file_name = table_name
  if modified_table_name: file_name = modified_table_name
  result = getAllFromTable(table_name)

  date = datetime.datetime.now()
  file_path = './output/{}_{}__T{}.txt'.format(file_name, date, len(result)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, result)


def getAllFromTable(table_name):
  sql = "SELECT * FROM {}".format(table_name)
  cur.execute(sql)
  result = cur.fetchall()
  return result


def saveIntoFile(file_path, result):
  print('Writing File => {}'.format(file_path) )
  file = open(file_path, 'w')
  rows = ''
  i = 0

  for index, row in enumerate(result):
    i += 1
    if len(row) == 1 and  row[0] != '': rows += str(row[0])
    elif len(row) == 2 and  row[1] != '': rows += str(row[1])
    else: rows += str('Empty Line Found Between Emails')
    
    if i == 50000 or index == len(result) - 1:
      print('{} writes'.format(index + 1))
      file.writelines(rows)
      rows = ''
      i = 0
    else: rows += '\n'
  file.close()


# Table Backup While Shuffle | Currently no Use
def backupTable(table_name, date):
  sql = "SELECT * FROM {}".format(table_name)
  cur.execute(sql)
  result = cur.fetchall()

  file_path = './output/backup_{}_{}.txt'.format(table_name, date).replace(" ", "_").replace(":", "-")
  file = open(file_path, 'w')

  for x in result: file.writelines(x[1])
  file.close()

def processInputToInsert (Lines):
  all_rows = ''
  
  for index, line in enumerate(Lines):
    row = "(NULL, '{}')".format(line[1])
    if index != len(Lines) - 1 and all_rows != '': all_rows += ', '
    all_rows += str(row)
  return all_rows
