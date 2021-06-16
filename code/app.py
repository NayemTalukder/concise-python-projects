import os
import datetime
import db

def processInputToInsert (Lines, table_name):
  all_rows = ''
  i = 0
  p = 0
  print('       0% => Reading . . . . .')
  
  for index, line in enumerate(Lines):
    if index >= (len(Lines) / 100) * p:
      p += 20
      print('       {}% => Reading . . . . .'.format(p) )
    i += 1
    row = "(NULL, '{}')".format(line.replace("\n", ""))
    if all_rows != '': all_rows += ', '
    all_rows += str(row)
    if i == 30000 or index == len(Lines) - 1:
      if all_rows != '': 
        try: db.insertIntoTable(table_name, all_rows)
        except: pass
      all_rows = ''
      i = 0


def read_input():
  db.create_temporary_tables()

  for x in range(1):
    print('input ({}) => Reading . . . . .'.format(x + 1))
    file_path = '../input/input ({}).txt'.format(x + 1)
    input = open(file_path, 'r')
    Lines = input.readlines()
    processInputToInsert(Lines, 'temp')
  db.processTemporaryEmail()


def get_feadback():
    print('TYPE  1  for GET Gmail')
    print('TYPE  2  for GET Other Email')
    type = int(input('TYPE HERE: '))

    if type == 1: table_name = 'gmail'
    else: table_name = 'other_email'
    
    print('TYPE  START  Value for {}'.format(table_name))
    start = int(input('TYPE HERE: '))
    
    print('TYPE  LIMIT  for {}'.format(table_name))
    limit = int(input('TYPE HERE: '))
    db.getChunkFromTable(table_name, start, limit)


def get_table_size():
    print('TYPE  1  for GET "Gmail" Table Size')
    print('TYPE  2  for GET "Other Email" Table Size')
    type = int(input('TYPE HERE: '))

    if type == 1: table_name = 'gmail'
    else: table_name = 'other_email'
    
    result = db.getAllFromTable(table_name)
    print('Size of Table {}: {}'.format(table_name, len(result)))


if __name__ == '__main__':
  print('TYPE  1  for INSERT new Email')
  print('TYPE  2  for GET Email')
  print('TYPE  3  for GET Table Size')
  action = int(input('TYPE HERE: '))

  if action == 1:
    confirm = input('TYPE  "CONFIRM"  INSERT new Email: ')
    if confirm == 'CONFIRM': read_input()
    else: print('MISTYPED')
    
  elif action == 2: get_feadback()
  elif action == 3: get_table_size()