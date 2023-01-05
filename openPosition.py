#! C:\Users\alpit\AppData\Local\Programs\Python\Python39\python.exe

print("Content-Type: text/html\n")


# Import packages
import sys


sys.path.append('/Test_AlphaOne')
import TS_Custom
from TS_Custom import *




vals=sys.argv[1].split(",")
print('VALS', vals)

openPosition(vals[0],vals[1],vals[2])




