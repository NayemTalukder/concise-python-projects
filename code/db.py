import sqlite3

con = sqlite3.connect("phone_number_storage.db")
cur = con.cursor()

def createTable(table_names):
  for i in table_names:
    sql =  """
        CREATE TABLE IF NOT EXISTS {} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          phone_number text NOT NULL UNIQUE
        );
      """.format(i)
    cur.execute(sql)

createTable(['phone_number'])

def insertIntoTable(table_name, rows):
  if rows != "":
    sql = "INSERT INTO {} VALUES {}".format(table_name, rows)
    try:
      cur.execute(sql)
      con.commit()
      return False
    except:
      return True