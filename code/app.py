from google.cloud import bigquery
import json, glob, os, random, shutil
import db, time

def processInputToInsert (Lines, table_name):
  insert_array = []
  rows_to_insert = []
  i = 0
  p = 0
  print('       0% => Reading . . . . .', end="\r")
  
  for index, line in enumerate(Lines):
    if index >= (len(Lines) / 100) * p:
      p += 20
      print('       {}% => Reading . . . . .'.format(p), end="\r" )
    i += 1
    row = '{}'.format(line.replace("\n", ""))
    rows_to_insert.append({u'email': row})
    if i == 50000 or index == len(Lines) - 1:
      if len(rows_to_insert) != 0: insert_array.append(rows_to_insert)
      rows_to_insert = []
      i = 0
  print('       {}% => Done                '.format(100) )
  return insert_array

def read_input():
  start_time = time.time()
  input_txt = glob.glob("input/*.txt")
  input_json = []
  i = 0
  for f in input_txt:
    # if i > 20: break
    i += 1
    print('{} => Reading . . . . .'.format(f))
    input = open(f, 'r')
    Lines = input.readlines()
    insert_array = processInputToInsert(Lines, 'temp')
    for j in insert_array: input_json.append(j)
  
  elapsed_time = int(time.time() - start_time)
  if elapsed_time < 60: db.countdown(60 - elapsed_time)
  db.insertIntoTable('temp', input_json)

if __name__ == '__main__':
    db.dropTable(['gmail', 'other_email', 'temp2', 'valid', 'duplicate', 'local_unique', 'global_unique'])
    db.createTable(['gmail', 'other_email', 'valid', 'temp', 'duplicate', 'local_unique', 'global_unique'])
    db.startProcess()
