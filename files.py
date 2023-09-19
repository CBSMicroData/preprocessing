#
# Copyright(c) 2023 Leiden University, Faculty of Sciences - LIACS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Authors:
#
#   Richard M.K. van Dijk
#   Research sofware engineer
#   E: m.k.van.dijk@liacs.leidenuniv.nl
#
#   Anton Schreuder
#   Postdoctoral researcher
#   E: a.schreuder@liacs.leidenuniv.nl
#
#   Leiden University,
#   Faculty of Math and Natural Sciences,
#   Leiden Institute of Advanced Computer Science (LIACS)
#   Snellius building | Niels Bohrweg 1 | 2333 CA Leiden
#   The Netherlands
#

import os
import os.path

import pyreadstat as sav
import string

#
# Package startup code
#
# The size of a file in bytes represents almost the required size of RAM assigned to the virtual machine
MAX_FILE_SIZE = 32000000000  # 32 Gigabytes 

global rootfolders

rootfolders = [
    "G:\Socialezekerheid\AKWUITKERING1ATAB",
    "G:\Socialezekerheid\AKWUITKERING1BTAB",
    "G:\Onderwijs\CITOTAB",
    "G:\Bevolking\GBABURGERLIJKESTAATBUS",
    "G:\Bevolking\GBAHUISHOUDENSBUS",
    "G:\Bevolking\GBAPERSOONTAB",
    "G:\Onderwijs\ONDERWIJSDEELNEMERSTAB"
]

#
# Functions
#
def getFiles(path, extension):
    dirs = []
    files = []
    scan = os.scandir(path)
    
    for entry in scan:
        if entry.is_dir():
            dirs.append(entry.name)
        if entry.is_file() and entry.name.lower().endswith(extension): # also compares uppercase extensions
            files.append(path + "\\" + entry.name)
            
    for dir in dirs:
        for file in getFiles(path + "\\" + dir, extension):
            files.append(file)
    
    return files

def createFileOfExtensionFiles(folders, extension):

    numberOfFiles = 0
    numberOfCategories = 0
    totalNumberOfFiles = 0
    totalNumberOfAccessibleFiles = 0
    totalNumberOfNonAccessFiles = 0

    f = open('accessible_files_proposal.txt', 'w', encoding="utf-8")
    n = open('non_access_files_proposal.txt', 'w', encoding="utf-8")

    for folder in folders:

        files = getFiles(folder, extension)
        numberOfFiles = len(files)
        
        numberOfCategories += 1
        totalNumberOfFiles = totalNumberOfFiles + numberOfFiles  # numberOfFile has two values ?

        print(folder)
        print(numberOfFiles)

        for file in files:
            
            print(file)
            
            try:
                g = open(file, 'r')  # Check accessibility because could run into permission denied errno 13
                g.close()

                f.write(file + '\n')
                totalNumberOfAccessibleFiles += 1

            except Exception as ex:
                n.write(file + '\n')
                totalNumberOfNonAccessFiles += 1
                print(ex)

    f.close()
    n.close()

    print("\n")
    print(f"Total number of files:             {str(totalNumberOfFiles)}")
    print(f"Total number of accessible files:  {str(totalNumberOfAccessibleFiles)}")
    print(f"Total number of non access files:  {str(totalNumberOfNonAccessFiles)}")
    print(f"Total number of categories:        {str(numberOfCategories)}")

    return

def printFilesInFolder(folder, extension):
    files = getFiles(folder, extension)
    for file in files:
        print(file)

def numberOfFilesInFolder(folder, extension):
    files = getFiles(folder, extension)
    return len(files)

# Search on root folder last chunk (index). Returns -1 if no chunk files created
def lastChunknrCsvFile(table):
    for chunknr in range(0,999):
        if not(os.path.exists(f"{table}.{chunknr}")):
            return chunknr - 1
    return chunknr

def removeChunkCsvFiles(table):
    for chunknr in range(0,999):
        if os.path.exists(f"{table}.{chunknr}"):
            os.remove(f"{table}.{chunknr}")
    return

#
# Examples of table name extraction from filename of sav file, versionname included
#
# 'G:\\Onderwijs\\CITOTAB.sav' -> CITO,  
# 'G:\\Bevolking\\GBASCHEIDINGENMASSATAB.sav' -> GBASCHEIDINGENMASSA
# 'G:\\Bevolking\\GBASCHEIDINGENMASSATAB\\2013\\140710 GBASCHEIDINGENMASSATAB 2013V1.sav' -> GBASCHEIDINGENMASSA2013V1
#
def getTableNameFromFileName(file):

    # Make lower case
    file = file.lower()

    # remove first characters until last "\\"
    index = file.rfind("\\") + 1
    file = file[index:]

    # remove .sav, .csv and spaces
    file = file.replace(".sav","")
    file = file.replace(".csv","")
    file = file.replace(" ","")

    # remove tab and bus
    file = file.replace("tab","")
    file = file.replace("bus","")

    # remove "1..... " with . a digit
    index = file.find("1")
    if index == 0:
        file = file[6:]

    # return upper case version
    return file.upper() + "_"

def readFiles(filename):

    # Converting the text file into a list of files
    with open(filename) as f:
        filesList = f.readlines()

    # Stripping "/n" from each line
    filesList = [line.strip() for line in filesList]

    return filesList

def createFileOfTableNames(fileList):

    nrFilesTooLargeForTransLog = 0

    f = open("tablenames_proposal.txt", "w")
    for file in fileList:
        
        size = os.stat(file).st_size
        if size > MAX_FILE_SIZE:
            nrFilesTooLargeForTransLog += 1
            
        f.write(str(size/1000000000) + " GB  " + file + " -> " + getTableNameFromFileName(file) + "\n")

    f.close()

    print(f"Total number of files too large:   {str(nrFilesTooLargeForTransLog)}")

    return

def getColumnNames(file):

    # read first row
    df, meta = sav.read_sav(file, row_limit=1, row_offset=0)
        
    columnNames = ""
    for name in meta.column_names:
        columnNames += name + "\n"
    
    return columnNames
    
def getColumnTypes(file):

    # read first row
    df, meta = sav.read_sav(file, row_limit=1, row_offset=0)
    
    columnTypes = ""
    keys = meta.original_variable_types.keys()
    for key in keys:
        columnTypes += meta.original_variable_types[key] + "\n"

    return

def createMetaDataOfSavFiles(fileList):

    nrFilesTooLargeForTransLog = 0

    f = open("metadata_proposal.txt", "w")
    for file in fileList:
        
        size = os.stat(file).st_size
        
        print(file + f" {size/1000000000} GB")
        
        f.write(f"{file} \n")
        f.write(f"Field column names:\n")
        f.write(f"{getColumnNames(file)} \n")
        
        f.write(f"Field column types:\n")
        f.write(f"{getColumnTypes(file)} \n")

    f.close()

    return

def delimiterCsvFile(file):
    f = open(file)

    if f.readline().find(',') > 0:
        f.close()
        return ','

    if f.readline().find(';') > 0:
        f.close()
        return ';'

    f.close()
    return ''

def columnsCsvFile(file):
    f = open(file)
    header = f.readline().split(delimiterCsvFile(file))
    f.close()
    columns = []
    for index in range(len(header)):
        st1 = header[index].replace('"','')
        st2 = st1.replace('\n','')
        columns.append(st2)
    return columns

#
# Call of functions
#
#  > import files as fs
#  > fs.createFileOfExtensionFiles(fs.rootfolders, '.sav')
#
#    ->  produces accessible_files_proposal.txt and none_accessible_files_proposal.txt
#
#  > fs.createFileOfTableNames(fs.readFiles("accessible_files_proposal.txt"))
#
#    ->  produces tablenames_proposal.txt file with size of the files included, to check the table name
#
#  > fs.createMetaDataOfSavFiles(fs.readFiles("accessible_files_proposal.txt"))
#
#    ->  produces meta data of sav files found in "accessible_files_proposal.txt".
#




