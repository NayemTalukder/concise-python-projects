import json, glob, os, random, shutil
ROOT_DIR = 'C:\\Users\\nayem\Desktop\\json_editor\\code\\input'

def generateJson(input_json, ran_images):
  train_json = {}
  val_json = {}
  mydata = json.load(open(input_json, "r"))
  for item in mydata:
    for key in mydata[item]:
      if key == 'filename': 
        if mydata[item]['filename'] in ran_images: val_json[item] = mydata[item]
        else: train_json[item] = mydata[item]
  with open('output/train.json', "w") as outfile: json.dump(train_json, outfile)
  with open('output/val.json', "w") as outfile: json.dump(val_json, outfile)

def splitRandomly():
  input_json = glob.glob("input/*.json")[0]
  limit = int(calculateLength([input_json])[0] * .4)
  ran_images = []  
  i = 0
  while i != limit:
    ran =  random.choice(os.listdir(ROOT_DIR)) 
    if ".json" not in ran and ".md" not in ran and ran not in ran_images:
      i = i + 1
      img = os.path.join(ROOT_DIR, 'val\\', ran)
      shutil.move(os.path.join(ROOT_DIR, ran), img)
      ran_images.append(ran)
  generateJson(input_json, ran_images)



def deleteTmageByType(image_type, keys_to_remove):
  count = 0
  read_inputs = glob.glob("input/*.jpg")
  if image_type == 'jpg': read_inputs = glob.glob("input/*.jpg")
  else: read_inputs = glob.glob("input/*.jpeg")
  for f in read_inputs: 
    if f.replace("input\\", "") not in keys_to_remove:
      os.remove(f)
      count = count + 1
  print(count)

def deleteImages(read_files):
  keys_to_remove = []
  for f in read_files:
    mydata = json.load(open(f, "r"))
    for item in mydata:
      for key in mydata[item]:
        if key == 'filename': keys_to_remove.append(mydata[item]['filename'])
  print('keys_to_remove:', len(keys_to_remove))
  deleteTmageByType('jpg', keys_to_remove)
  deleteTmageByType('jpeg', keys_to_remove)

  

def mergeFiles(read_files):
  items = {}
  output_file = ''
  for f in read_files:
    output_file = output_file + '__'+ f.replace("input\\", "").replace(".", "_")
    my_json = open(f, "r")
    mydata = json.load(my_json)
    for item in mydata:
      items[item] = mydata[item]

  output_file = 'output/merged{}.json'.format(output_file)
  with open(output_file, "w") as outfile: json.dump(items, outfile)


def deleteItemWithEmptyRegions(read_files):
  for f in read_files:
    mydata = json.load(open(f, "r"))
    keys_to_remove = []

    for item in mydata:
      for key in mydata[item]:
        if key == 'regions' and len(mydata[item][key]) == 0: keys_to_remove.append(item)

    for key in keys_to_remove: mydata.pop(key)
    output_file = 'output/out_{}'.format(f.replace("input\\", ""))
    with open(output_file, 'w') as f: json.dump(mydata, f)


def calculateLength (read_files):
  len_arr = []
  for f in read_files:
    try:
      mydata = json.load(open(f, "r"))
      print(f, '=' , len(mydata))
      len_arr.append(len(mydata))
    except: print(f, '=' , 0)
  return len_arr
  

if __name__=='__main__':
  read_inputs = glob.glob("input/*.json")
  read_outputs = glob.glob("output/*.json")
  print("1 for length (input)")
  print("2 for length (output)")
  print("3 for delete empty regions")
  print("4 for merge json")
  print("6 for Split Randomly")
  i = int(input("Press a Key: "))
  if i == 1: calculateLength(read_inputs)
  elif i == 2: calculateLength(read_outputs)
  elif i == 3: deleteItemWithEmptyRegions(read_inputs)
  elif i == 4: mergeFiles(read_inputs)
  elif i == 5: deleteImages(read_inputs)
  elif i == 6: splitRandomly()