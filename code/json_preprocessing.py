import json, glob, os


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



def getAllImages():
  all_images = []
  read_inputs = glob.glob("input/*")
  for f in read_inputs: 
    if ".json" not in f and ".md" not in f: 
      all_images.append( f.replace("input\\", ""))
  return all_images

def deleteExtraAnnotations(read_files):
  output_json = {}
  all_images = getAllImages()
  
  for f in read_files: 
    mydata = json.load(open(f, "r"))
    for item in mydata:
      for key in mydata[item]:
        if key == 'filename': 
          if mydata[item]['filename'] in all_images: output_json[item] = mydata[item]
  with open('output/without_extra_annotations.json', "w") as outfile: json.dump(output_json, outfile)

  

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
  