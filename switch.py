import os
import time

folder_log = "./logs"
number_countries  = 7
os.system(f"rm  {folder_log}/*")#remove all the files in folder log

while True:
    num_files = len([f for f in os.listdir(folder_log) if f.endswith('.txt')])
    print(f"{num_files} out of {number_countries} collectors are done...")
    if num_files == number_countries:
        print("Collection completed! Switching off ...")
        os.system("sudo poweroff")
    time.sleep(600) # pause for 10 mins before checking again
