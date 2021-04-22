import argparse
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join
import fnmatch
import json
import re
from datetime import datetime
import pandas as pd 
from pandas.io.json import json_normalize 
from pathlib import Path
import sys
import time
import os



print("*****Batch start*****")

#handling arguments part
parser = argparse.ArgumentParser()
parser.add_argument("dir", help="Enter directory to be read")
#this is an optional argument, you can call it using the dest varible "unix"
parser.add_argument("-u", action="store_true", dest="unix", default=False,
                    help="Count number of character in word")
args = parser.parse_args()
#print(args.dir)



#The following list will contain the files that were already read before
filesAlreadyRead = list()
#This creates a checkpoint file if it doesn't exist and throws no errors if it exists already
myfile = Path(args.dir+'checkpoint.txt')
myfile.touch(exist_ok=True)
#Getting files that were read previosuly from the checkpoint file to avoid writing the same files again
with open(myfile) as filehandle:
    filesAlreadyRead = [current_place.rstrip() for current_place in filehandle.readlines()]

#Checking for duplicates
checksums = {}
duplicates = list()
filesToRead = list()
try:
    files = [item for item in listdir(args.dir) if isfile(join('.', item))]
except:
    print("You might have entered a wrong directory path")
    sys.exit()
for filename in files:
    #Here I'm looking for files that end with ".json"
    if fnmatch.fnmatch(filename, '*.json'):
        #print(filename)
        with Popen(["md5sum", filename], stdout=PIPE) as proc:
            checksum = proc.stdout.read().split()[0]
            #Checking to see if the file is a duplicate or not
            if checksum in checksums:
                duplicates.append(filename)
            else:
                #Here, I'm checking that the file wasn't read before in order to append it to the filesToRead list
                if filename not in filesAlreadyRead:
                    filesToRead.append(filename)
            checksums[checksum] = filename

#Some debugging prints
#print(checksums)    
#print(duplicates)
#print(filesToRead)

#Printing the number of duplicate files in the current batch
print("Found", len(duplicates), "duplicate file(s) in this batch")

#Deleting duplicate files
if len(duplicates) > 0:
    print("Deleting duplicate files ...")
    for filename in duplicates:
        os.remove(filename)
    print("Duplicate files deleted successfully!")

#Creating an output directory if not exists, if it exists no errors will be thrown
p = Path(args.dir+'Output_dir/')
p.mkdir(exist_ok = True)
    
    
#reading and parsing json files
jsonList = list()
row = {}
data = list()
for filename in filesToRead:
    print("Working on file", filename)
    with open(filename) as handle:
        for line in handle:
            data.append(json.loads(line))
        #print(type(data))
        #print(data)
        for record in data:
            #print(type(record))
            try:
                browser = record['a'].split('/')[0]
            except:
                browser = None
            #print(browser)
            try:
                #The following regex looks for a string that starts with a bracket then non-whitespace character \n
                # and one or more characters followed by a whitespace character
                #The function re.findall() returns a list, that's why I'm returning the first element
                os = re.findall('\((\S[a-z]+)\s', record['a'])[0]
            except:
                os = None
            #print(os)
            try:
                #The following regex looks for a string that starts with double forward slashes then a one or more \n
                # characters capital or small with any character followed by a non-whitespace character and ends with \n
                # a forward slash.
                from_url = re.findall('\/\/([a-zA-Z.\S]+?)\/',record['r'])[0]
            except:
                from_url = None
            #print(from_url)
            try:
                to_url = re.findall('\/\/([a-zA-Z.\S]+?)\/',record['u'])[0]
            except:
                to_url = None
            #print(to_url)
            try:
                city = record['cy']
            except:
                city = None
            #print(city)
            try:
                longitude = record['ll'][0]
                latitude = record['ll'][1]
            #print(longitude, latitude)
            except:
                longitude = None
                latitude = None
            #print(longitude, latitude)
            try:
                if len(record['tz']) < 1:
                    timezone = None
                else:
                    timezone = record['tz']
            except:
                timezone = None
            #print(timezone)
            #Checking to see if user wants to print time as a Unix timestamp or in human readable format
            try:
                if args.unix:
                    time_in = record['t']
                    time_out = record['hc']
                else:
                    time_in = datetime.fromtimestamp(record['t'])
                    time_out = datetime.fromtimestamp(record['hc'])
            except:
                time_in = None
                time_out = None
            #print(time_in)
            #print(time_out)
            #Constructing a row dictionary that contains the wanted info
            row = {"web_browser":browser, "operating_sys":os, "from_url":from_url, "to_url":to_url, "city":city, "long":longitude, "lat":latitude, "timezone":timezone, "time_in":time_in, "time_out":time_out}
            #print(row)
            #Appending each row to a json list in order to convert it to a datafram later
            jsonList.append(row)
            #print(jsonDict)
    #Inside the filelist loop        
    df = json_normalize(jsonList)
    #print("json is converted to a df")
            
    df.fillna("Unknown", inplace=True)
    #print("null values are handled")
    
    #Changing the file name for better readability
    newFileName = filename.replace(".json", "_READY.csv")
    
    print("No. of rows transformed is", len(df.index), "rows")
    print("File path is", args.dir+'Output_dir/'+newFileName)
    
    #Writing the dataframe to a .csv file
    df.to_csv(args.dir+'Output_dir/'+newFileName)
    filesAlreadyRead.append(filename)
    
    #Clearing lists and df for the next file in the list
    jsonList = []
    df = df[0:0]
    data = []
                  
#Writing the files that were read in this batch to the checkpoint file
with open(args.dir+'checkpoint.txt', 'w') as filehandle:
    filehandle.writelines("%s\n" % line for line in filesAlreadyRead)
    
#Printing the execution time
start_time = time.perf_counter()
print("Execution time is {0:.10f}".format(time.perf_counter() - start_time), "seconds")
print("*****End of batch at", datetime.today().strftime('%Y-%m-%d-%H:%M:%S'), "*****")
print("")



        



