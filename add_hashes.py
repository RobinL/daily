import os
import pandas as pd
import hashlib

my_dir = r"/home/ubuntu/datasets/combined_for_processing"

my_files = os.listdir(my_dir)
my_files = [my_file for my_file in my_files if r".csv" in my_file]
my_files = [os.path.join(my_dir,my_file) for my_file in my_files]

for my_file in my_files:
    print(my_file)
    df = pd.read_csv(my_file)
    df["hash"] =df.apply(lambda x: hashlib.md5(",".join([str(y) for y in x])).hexdigest(),axis=1)
    df.to_csv(my_file, encoding="utf-8",index=False)
    del df