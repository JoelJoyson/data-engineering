# data-engineering

Simple Job to create tables, read files and load the data into files


To create a new records table for a new country:
1. add a .txt file in the src/data_loader/data_files folder
2. execute the dataLoader script under the data_loader package


We can run job runs in 2 modes:
 1. Batch -for all the files in the directory
        - python dataLoader.py --mode 'batch'
 2. Only for a specific file/countries in the directory
        - python dataLoader.py --country 'India,Germany'