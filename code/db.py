import sqlite3
import datetime
import math

con = sqlite3.connect("email_storage.db")
cur = con.cursor()

def createTable(table_names):
  for i in table_names:
    sql =  """
      CREATE TABLE {} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email text NOT NULL
      );
    """.format(i)
    cur.execute(sql)


def dropTable(table_names):
  for i in table_names:
    sql = 'DROP TABLE IF EXISTS {}'.format(i)
    cur.execute(sql)


def create_temporary_tables():
  # Dropping Tables { IF EXISTS } 
  print('{ IF EXISTS } Dropping Tables . . . . .') 
  dropTable(['temp', 'invalid', 'duplicate', 'unique_gmail', 'temp_temp', 'unique_other_email'])

  # Creating Tables
  print('Creating Tables . . . . .') 
  createTable(['temp', 'invalid', 'duplicate', 'unique_gmail', 'temp_temp', 'unique_other_email'])


# [START] => Process Input
def processTemporaryEmail():
  print('[START] => Processing Starts . . . . .')
  processInvalidEmail()
  
  temp_table_length = getTableLength('temp')
  cycles_needed = math.ceil(temp_table_length / 500000) + 1
  print('[START] => Processing Cycle Starts . . . . .')
  i = 1
  current_cycle = 0
  while i >= 1:
    current_cycle += 1
    i = processingCycle(current_cycle, cycles_needed)

  print('[END] => Processing Cycle Ends . . . . .')
  # Save Duplicate and Unique Emails to file
  getAllFromTable('unique_gmail', 'unique')
  getAllFromTable('duplicate', False)
  # Seperate Otrher Emails From Gmail
  processUniqueGmailTable()
  print('[END] => Processing Ends . . . . .')


def processInvalidEmail():
  print('Invalid Email Processing . . . . .')
  cur.execute("Select * from temp where email NOT Like '%@%'")
  data = cur.fetchall()

  date = datetime.datetime.now()
  file_path = '../feadback/{}_{}__T{}.txt'.format('invalid', date, len(data)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, data)
  deleteChunkFromTable('temp', data)


def processingCycle (current_cycle, cycles_needed):
  print('{} / {} => Processing Cycle Running . . . . .'.format(current_cycle, cycles_needed))
  sql = "SELECT * FROM temp LIMIT 500000"
  cur.execute(sql)
  result = cur.fetchall()

  if len(result) != 0:
    InsertFromTableToTable('temp', 'temp_temp', result, True)
    processTempTempTable(current_cycle)

  return len(result)


def processTempTempTable(current_cycle):
  print('TempTemp Table Processing for Cycle => {} . . . . .'.format(current_cycle))
  sql = "SELECT * FROM temp_temp GROUP BY email"
  cur.execute(sql)
  result = cur.fetchall()
  InsertFromTableToTable('temp_temp', 'unique_gmail', result, True)
  
  sql = "SELECT * FROM temp_temp"
  cur.execute(sql)
  result = cur.fetchall()
  InsertFromTableToTable('temp_temp', 'duplicate', result, True)


def processUniqueGmailTable():
  print('Unique Email Processing . . . . .')
  sql = "Select * from unique_gmail where email not Like '%@gmail.com%';"
  cur.execute(sql)
  result = cur.fetchall()
  InsertFromTableToTable('', 'unique_other_email', result, False)
# [END] => Process Input


def getTableLength(table_name):
  cur.execute("Select * from {}".format(table_name))

  data = cur.fetchall()
  return len(data)


def InsertFromTableToTable (from_table, to_table, result, delete_flag):
  rows = ""
  ids = ""
  i = 0
  for index, row in enumerate(result):
    i += 1
    if rows != '':
      rows += ', '
      ids += ', '
    rows += str(row)
    ids += str(row[0])
  
    if i == 25000 or index == len(result) - 1:
      insertIntoTable(to_table, rows)
      if delete_flag:
        sql = "DELETE FROM {} WHERE id IN ({})".format(from_table, ids)
        cur.execute(sql)
        con.commit()
      rows = ""
      ids = ""
      i = 0


def insertIntoTable(table_name, rows):
  # if table_name == 'temp_temp':
    # print(table_name)
    # print(rows)

  if rows != "":
    sql = "INSERT INTO {} VALUES {}".format(table_name, rows)
    cur.execute(sql)
    con.commit()


def getChunkFromTable(table_name, start, limit):
  sql = "SELECT * FROM {} WHERE id >= {} LIMIT {}".format(table_name, start, limit)
  cur.execute(sql)
  result = cur.fetchall()

  date = datetime.datetime.now()
  file_path = '../feadback/{}_{}__S{}-E{}_T{}.txt'.format(table_name, date, start, start+len(result)-1, len(result)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, result)


def getAllFromTable(table_name, modified_table_name):
  file_name = table_name
  if modified_table_name: file_name = modified_table_name
  sql = "SELECT * FROM {}".format(table_name)
  cur.execute(sql)
  result = cur.fetchall()

  date = datetime.datetime.now()
  file_path = '../feadback/{}_{}__T{}.txt'.format(file_name, date, len(result)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, result)


def saveIntoFile(file_path, result):
  print(file_path)
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


def deleteChunkFromTable(table_name, result):
  print(table_name)
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



# Table Backup While Shuffle | Currently no Use
def backupTable(table_name, date):
  sql = "SELECT * FROM {}".format(table_name)
  cur.execute(sql)
  result = cur.fetchall()

  file_path = '../feadback/backup_{}_{}.txt'.format(table_name, date).replace(" ", "_").replace(":", "-")
  file = open(file_path, 'w')

  for x in result: file.writelines(x[1])
  file.close()

# Shuffle Data Tables | Currently no Use
def shuffleRows(table_name):
  date = datetime.datetime.now()
  backupTable(table_name, date)

  create_table = "CREATE TABLE demo ( id int NOT NULL AUTO_INCREMENT, email varchar(255), primary key (id) )"
  cur.execute(create_table)

  i = 1
  while i >= 1:
    sql = "SELECT * FROM {} ORDER BY RAND () LIMIT 25000".format(table_name)
    cur.execute(sql)
    result = cur.fetchall()

    if len(result) != 0: InsertFromTableToTable (table_name, 'demmo', result, True)
    i = len(result)

  # # Drop Current Table
  # sql = 'DROP TABLE {}'.format(table_name)
  # cur.execute(sql)

  # # Rename Existing Table
  # sql = 'ALTER TABLE {} RENAME TO {}'.format('temp', table_name)
  # cur.execute(sql)
