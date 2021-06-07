import mysql.connector

def connect(db_name):
  return mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database=db_name
  )

if __name__ == '__main__':
  db = connect('')
  cursor = db.cursor()

  # Drop Database
  try:
    db = connect('email_storage')
    cursor = db.cursor()
    cursor.execute('DROP DATABASE email_storage')
  except: pass

  # Create Dtabase
  cursor.execute("CREATE DATABASE email_storage")

  # Create Tables
  db = connect('email_storage')
  cursor = db.cursor()

  create_table = "CREATE TABLE gmail ( id int NOT NULL AUTO_INCREMENT, email varchar(255), primary key (id) )"
  cursor.execute(create_table)

  create_table = "CREATE TABLE other_email ( id int NOT NULL AUTO_INCREMENT, email varchar(255), primary key (id) )"
  cursor.execute(create_table)