#!/bin/bash
#Change directory to the directory containing your python script
cd /home/eslam/Downloads/ITI_Python/ITI_Python_for_Data_Management/Task2
#Write the path to where your python is executed (you can use the command >>which python<< to know where your python is executed)
#After that write the name of your python script followed by the path to the directory containing your data
#The >>tee<< command is to redirect output to a file (created if not exists) and the -a is to append to the file
/home/eslam/anaconda3/bin/python jsonLoader.py /home/eslam/Downloads/ITI_Python/ITI_Python_for_Data_Management/Task2/ | tee -a execution_logger.log

