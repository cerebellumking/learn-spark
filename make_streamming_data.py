import random 
import os 
import time 
import shutil
sentences = ["eat tensorflow2 in 30 days","eat pytorch in 20 days","eat pyspark in 10 days"]
data_path = "./data/streamming_data"

if os.path.exists(data_path):
    shutil.rmtree(data_path)
    
os.makedirs(data_path)

for i in range(20):
    line = random.choice(sentences)
    tmp_file = str(i)+".txt"
    with open(tmp_file,"w") as f:
        f.write(line)
        f.flush()
    shutil.move(tmp_file,os.path.join(data_path,tmp_file))
    time.sleep(1)
    
