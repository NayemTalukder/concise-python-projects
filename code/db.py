from google.cloud import bigquery
import datetime
import json, glob, os, random, shutil
import time

# Credential Initialization
credentials_path = 'C:\\Users\\nayem\\Desktop\\BigQueryPython\\code\\credential.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Global Variables
client = bigquery.Client()
PROJECT_NAME = 'bupkoplayground'
DATASET_NAME = 'demo'

# bupkoplayground.demo

INPUT = 'C:\\Users\\nayem\\Desktop\\json_editor\\code\\input'
OUTPUT = 'C:\\Users\\nayem\\Desktop\\json_editor\\code\\output'

def countdown(t):
  while t:
    mins, secs = divmod(t, 60)
    timer = 'Waiting Time => {:02d}:{:02d}'.format(mins, secs)
    print(timer, end="\r")
    time.sleep(1)
    t -= 1

  print('Waiting Time => 00:00')


def dropTable(table_id):
    for t in table_id:
        table_name = PROJECT_NAME + '.' + DATASET_NAME + '.' + t
        client.delete_table(table_name, not_found_ok=True)
        print( "Drop table {}".format(table_name) )

def createTable(table_id):
    for table_name in table_id:
        schema = [
            bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        ]

        table_ref = PROJECT_NAME + '.' + DATASET_NAME + '.' + table_name
        table = bigquery.Table(table_ref, schema=schema)
        try: 
            table = client.create_table(table)
            print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id) )
        except: print( "Table Already Exists. . . ." )

def insertIntoTable(table_name, rows_to_insert):
  table_ref = PROJECT_NAME + '.' + DATASET_NAME + '.' + table_name
  c_len = len(rows_to_insert)
  print('Inserting . . . . . . . .  .  . . .')
  print('       0.00% => 1 / {} Progressing . . .'.format(c_len), end="\r")
  for i, r in enumerate(rows_to_insert):
    percentage = (100 / c_len) * i
    print('       {:.2f}% => {} / {} Progressing . . .'.format(percentage, i, c_len), end="\r")
    client.insert_rows_json(table_ref, r)

def tempPerformQuery (table_name):
    table_ref = PROJECT_NAME + '.' + DATASET_NAME + '.' + table_name

    query = """
        -- SELECT * FROM `bupkoplayground.demo.duplicate` LIMIT 1000
        -- INSERT INTO `bupkoplayground.demo.demo` VALUES ('aaaaaaaa')
        -- SELECT DISTINCT email FROM `bupkoplayground.demo.demo`

        INSERT INTO `bupkoplayground.demo.temp_gmail`
        SELECT email FROM `bupkoplayground.demo.demo` LIMIT 1000
        WHERE email NOT IN (
            SELECT email FROM `bupkoplayground.demo.duplicate`
        )
    """

    sql = """
        Select *
        From (
            Select Row_Number() Over (Order By email) As RowNum, *
            From `bupkoplayground.demo.valid`
        )
        Where RowNum > 5 LIMIT 10;
    """

    QUERY = 'SELECT * FROM `bupkoplayground.demo.demo` WHERE email != "" LIMIT 1000'
    query_job = client.query(QUERY)
    rows = query_job.result()

    for row in rows: print(row.email)

def performQuery (sql, no_feadback=False):
  if no_feadback: client.query(sql)
  else:
    query_job = client.query(sql)
    data = query_job.result()
    rows = list(data)
    return rows

def saveIntoFile(file_name, result):
  date = datetime.datetime.now()
  file_path = './output/{}_{}__T{}.txt'.format(file_name, date, len(result)).replace(" ", "_").replace(":", "-")
  
  print('Writing File => {}'.format(file_path) )
  file = open(file_path, 'w')
  rows = ''
  i = 0

  for index, row in enumerate(result):
    i += 1
    if row.email != '': rows += str(row.email)
    else: rows += str('Empty Line Found Between Emails')
    
    if i == 50000 or index == len(result) - 1:
      print('{} writes'.format(index + 1))
      file.writelines(rows)
      rows = ''
      i = 0
    else: rows += '\n'
  file.close()

def startProcess():
  print('[START] => Processing Starts . . . . .')
  filterInvalidEmail()
  filterDuplicateEmail()
  countdown(90)
  insertIntoMainTable()
  generateFeadBackFiles()

def filterInvalidEmail():
  print('Filtering Invalid Emails . . . . .')
  sql = """
      INSERT INTO `bupkoplayground.demo.valid`
      SELECT * FROM `bupkoplayground.demo.temp`;
      
      DELETE FROM `bupkoplayground.demo.valid` 
      WHERE 
          email NOT LIKE '%.com' OR
          email LIKE '%..%' OR
          email NOT LIKE '%@%';

      SELECT * FROM `bupkoplayground.demo.temp` 
      WHERE 
          email NOT LIKE '%.com' OR
          email LIKE '%..%' OR
          email NOT LIKE '%@%';
  """
  rows = performQuery(sql)
  saveIntoFile('invalid', rows)
  
def filterDuplicateEmail():
  print('Filtering Duplicate Emails . . . . .')

  sql = """
      -- [START] => Filter Local Duplicate
        -- { Copy DISTINCT email to [local_unique] }
      INSERT INTO `bupkoplayground.demo.local_unique`
      SELECT DISTINCT email FROM `bupkoplayground.demo.valid`;

        -- { Create [temp2] Table }
      CREATE TABLE `bupkoplayground.demo.temp2` (
          email STRING,
          ID INTEGER
      );

        -- { Copy All emails to [temp2] }
      INSERT INTO `bupkoplayground.demo.temp2`
      Select email, ROW_NUMBER() over (Order by email ASC) as UniqueKey 
      from `bupkoplayground.demo.valid`;

        -- { Copy All Duplicate emails to [duplicate] }
      INSERT INTO `bupkoplayground.demo.duplicate`
      SELECT E.email FROM `bupkoplayground.demo.temp2` E
      INNER JOIN (
          SELECT *, 
                  RANK() OVER(PARTITION BY email
                  ORDER BY ID) rank
          FROM `bupkoplayground.demo.temp2`
      ) T ON E.ID = T.ID
      WHERE rank > 1;
      -- [END] => Filter Local Duplicate


      -- [START] => Filter Global Duplicate
      INSERT INTO `bupkoplayground.demo.global_unique`
      SELECT * FROM `bupkoplayground.demo.local_unique`
      WHERE email NOT IN (
        SELECT email FROM `bupkoplayground.demo.gmail` 
        UNION DISTINCT
        SELECT email FROM `bupkoplayground.demo.other_email`
      );

      DELETE FROM `bupkoplayground.demo.local_unique`
      WHERE email NOT IN (
        SELECT * FROM `bupkoplayground.demo.gmail` 
        UNION DISTINCT
        SELECT * FROM `bupkoplayground.demo.other_email`
      );

      INSERT INTO `bupkoplayground.demo.duplicate`
      SELECT * FROM `bupkoplayground.demo.local_unique`;
      -- [END] => Filter Global Duplicate
  """
    
  performQuery(sql, True)

def insertIntoMainTable():
  print('Inserting Into Main Table . . . . .')

  sql = """
      INSERT INTO `bupkoplayground.demo.gmail`
      SELECT * FROM `bupkoplayground.demo.global_unique` 
      WHERE email LIKE '%@gmail.com%';

      
      INSERT INTO `bupkoplayground.demo.other_email`
      SELECT * FROM `bupkoplayground.demo.global_unique` 
      WHERE email NOT LIKE '%@gmail.com%';
  """

  performQuery(sql, True)

def generateFeadBackFiles():
  print('Generating Duplicate Emails . . . . .')
  sql = """
      SELECT * FROM `bupkoplayground.demo.duplicate` LIMIT 200000 
  """
  rows = performQuery(sql)
  saveIntoFile('duplicate', rows)


  print('Generating Unique Gmails . . . . .')
  sql = """
      SELECT * FROM `bupkoplayground.demo.global_unique` 
      WHERE email LIKE '%@gmail.com%' LIMIT 200000;
  """
  rows = performQuery(sql)
  saveIntoFile('gmail', rows)

  
  print('Generating Unique Other Emails . . . . .')
  sql = """
      SELECT * FROM `bupkoplayground.demo.global_unique` 
      WHERE email NOT LIKE '%@gmail.com%' LIMIT 200000;
  """
  rows = performQuery(sql)
  saveIntoFile('other_email', rows)
    