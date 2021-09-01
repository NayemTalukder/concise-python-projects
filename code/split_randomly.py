import json, glob, os, random, shutil
from pathlib import Path
from os.path import join as joinP
import json_preprocessing

INPUT = 'C:\\Users\\nayem\Desktop\\json_editor\\code\\input'
OUTPUT = 'C:\\Users\\nayem\Desktop\\json_editor\\code\\output'

def generateJson(input_json, val_images):
  train_json = {}
  val_json = {}
  mydata = json.load(open(input_json, "r"))
  for item in mydata:
    for key in mydata[item]:
      if key == 'filename': 
        if mydata[item]['filename'] in val_images: val_json[item] = mydata[item]
        else: train_json[item] = mydata[item]
  with open('output/train/via_region_data.json', "w") as outfile: json.dump(train_json, outfile)
  with open('output/val/via_region_data.json', "w") as outfile: json.dump(val_json, outfile)

def generateTrain():
  train_images = glob.glob("input/*")
  for f in train_images:
    if ".json" not in f and ".md" not in f:
      j = f.split('\\')[1]
      shutil.move(joinP(INPUT, j), joinP(OUTPUT, 'train\\', j))

def generateVal(input_json):
  limit = int(json_preprocessing.calculateLength([input_json])[0] * .4)
  val_images = []
  i = 0
  while i != limit:
    ran =  random.choice(os.listdir(INPUT))
    if ".json" not in ran and ".md" not in ran and ran not in val_images:
      i = i + 1
      shutil.move(joinP(INPUT, ran), joinP(OUTPUT, 'val\\', ran))
      val_images.append(ran)
  return val_images

def removeAndCreateDir(Dirs):
  for d in Dirs:
    if os.path.isdir(joinP(OUTPUT, d)): shutil.rmtree(joinP(OUTPUT, d))
    Path(joinP(OUTPUT, d)).mkdir(parents=True, exist_ok=True)

def splitRandomly():
  input_json = glob.glob("input/*.json")[0]
  removeAndCreateDir(['train', 'val'])
  val_images = generateVal(input_json)
  generateTrain()
  generateJson(input_json, val_images)
