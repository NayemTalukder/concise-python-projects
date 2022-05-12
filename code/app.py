from random import randint, choice
import datetime, db

def saveIntoFile(file_path, result):
  print('Writing File => {}'.format(file_path) )
  file = open(file_path, 'w')
  rows = ''
  i = 0

  for index, row in enumerate(result):
    i += 1
    rows += str(row)
    
    if i == 50000 or index == len(result) - 1:
      print('{} writes'.format(index + 1))
      file.writelines(rows)
      rows = ''
      i = 0
    else: rows += '\n'
  file.close()

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

if __name__ == '__main__':
  print(' 1  for Grameeen Phone')
  print(' 2  for Banglalink')
  print(' 3  for Airtel')
  print(' 4  for Robi')
  print(' 5  for Teletalk')
  while True:
    operator = input('Choose an operator: ')
    if operator in ['1', '2', '3', '4', '5']: break
    else: print('\nWrong Input. Please Give "Correct" Input.\n')

  if operator == '1':
    operator_name = 'Grameeen Phone'
    print('\n 3  for "013" Series')
    print(' 7  for "017" Series')
    
    while True:
      series = input('Choose an Series: ')
      if series in ['3', '7']: break
      else: print('\nWrong Input. Please Give "Correct" Input.\n')

    if series == '3': prefix = '013'
    else: prefix = '017'
  elif operator == '2':
    operator_name = 'Banglalink'
    print(' 4  for "014" Series')
    print(' 9  for "019" Series')
    
    while True:
      series = input('Choose an Series: ')
      if series in ['4', '9']: break
      else: print('\nWrong Input. Please Give "Correct" Input.\n')

    if series == '4': prefix = '014'
    else: prefix = '019'
  elif operator == '3':
    operator_name = 'Airtel'
    prefix = '016'
  elif operator == '4':
    operator_name = 'Robi'
    prefix = '018'
  elif operator == '5':
    operator_name = 'Teletalk'
    prefix = '015'
    
  while True:
    totalOutput = input('Specify Total Output: ')
    errorFlag = False
    for i in totalOutput:
      if i not in ['0', '1', '2', '3', '4', '5', '6', '7','8', '9']: 
        errorFlag = True
        break
    if errorFlag: print('\nWrong Input. Please Give "Correct" Input.\n')
    else:
      totalOutput = int(totalOutput)
      break

  number_list = []
  while totalOutput > 0:
    phone_number = prefix + str(random_with_N_digits(8))
    is_duplicate = db.insertIntoTable( 'phone_number', str("(NULL, '{}')".format(phone_number)) )
    if is_duplicate is not True:
      number_list.append(phone_number)
      totalOutput -= 1

  date = datetime.datetime.now()
  file_path = '../feadback/{}_numbers_{}__T{}.txt'.format(operator_name, date, len(number_list)).replace(" ", "_").replace(":", "-")
  saveIntoFile(file_path, number_list)