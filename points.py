from random import seed
from random import random
import sys
import os

n = 50  #sys.argv[0]
dim = 2 #sys.argv[1]
out_dir = './dataset'
if not os.path.exists(out_dir):
  os.makedirs(out_dir)         
filename = 'points_' + str(dim) + '_' + str(n) + '.csv'
file_path = os.path.join(out_dir,filename) 

seed(42)

f =  open(file_path,'w')
for _ in range(n):
  values = [100*random() for i in range(dim) ]
  for i in range(dim):
    f.write(str(values[i]))
    if i!=dim-1:
        f.write(',')
  f.write('\n')
f.close()
