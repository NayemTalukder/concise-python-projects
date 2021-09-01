import glob
import json_preprocessing
import split_randomly

if __name__=='__main__':
  read_inputs = glob.glob("input/*.json")
  read_outputs = glob.glob("output/*.json")
  print("1 for length (input)")
  print("2 for length (output)")
  print("3 for delete empty regions")
  print("4 for delete extra annotations")
  print("5 for merge json")
  print("6 for delete unwanted images")
  print("7 for split Randomly")
  i = int(input("Press a Key: "))
  if i == 1: json_preprocessing.calculateLength(read_inputs)
  elif i == 2: json_preprocessing.calculateLength(read_outputs)
  elif i == 3: json_preprocessing.deleteItemWithEmptyRegions(read_inputs)
  elif i == 4: json_preprocessing.deleteExtraAnnotations(read_inputs)
  elif i == 5: json_preprocessing.mergeFiles(read_inputs)
  elif i == 6: json_preprocessing.deleteImages(read_inputs)
  elif i == 7: split_randomly.splitRandomly()