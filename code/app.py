import os
import datetime
import db

def feadback_handling(file_name, email, date, count):
  file_path = '../feadback/{}_{}.txt'.format(file_name, date).replace(" ", "_").replace(":", "-")
  
  if count:
    new_path = '../feadback/{}_{}__{}.txt'.format(file_name, date, count).replace(" ", "_").replace(":", "-")
    os.rename(file_path, new_path)
  else:
    file = open(file_path, 'a')
    file.writelines(email)
    file.close()


def processInputToInsert (Lines, table_name):
  all_rows = ''
  i = 0
  
  for index, line in enumerate(Lines):
    i += 1
    row = "(NULL, '{}')".format(line.replace("\n", ""))
    if all_rows != '': all_rows += ', '
    all_rows += str(row)
    if i == 30000 or index == len(Lines) - 1:
      try:
        if all_rows != '': db.insertIntoTable(table_name, all_rows)
      except: pass
      all_rows = ''
      i = 0


def read_input():
  db.create_temporary_tables()

  for x in range(1):
    print('input ({}) => Inserting . . . . .'.format(x + 1))
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
    db.getFromTable(table_name, start, limit)


if __name__ == '__main__':
  print('TYPE  1  for INSERT new Email')
  # print('TYPE  2  for GET Email')
  # print('TYPE  3  for SHUFFLE')
  action = int(input('TYPE HERE: '))

  if action == 1: read_input()
  # elif action == 2: get_feadback()
  # elif action == 3:
  #   SHUFFLE = input('Please TYPE "SHUFFLE" to confirm: ')
  #   if SHUFFLE == 'SHUFFLE':
  #     db.shuffleRows('gmail')
  #     db.shuffleRows('other_email')
  #   else: print('MISMATCH')