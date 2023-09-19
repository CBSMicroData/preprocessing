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
import gc
import files as fs
import pandas as pd
import pyodbc as db
import urllib.parse
from sqlalchemy import create_engine
import pyreadstat as sav
import string

#
# Setup the database connection
#

from sqlalchemy import event

#
# Package startup code
#
global CHUNKSIZE
CHUNKSIZE = 1000000              # The size of the chunks while converting sav files

global MAX_SAV_FILE_SIZE
MAX_SAV_FILE_SIZE = 32000000000  # 32 Gigabyte

global sqlserver
global engine
global cursor

driver   = 'DRIVER={ODBC Driver 13 for SQL Server};'
server   = 'SERVER=S0DSQL0141B\I01;'
database = 'DATABASE=RA<fill in project number CBS>;'
username = 'UID=RA<fill in project number CBS>;'
password = 'PWD=<fill in your DB general password'

connectString = driver + server + database + username + password
sqlserver = db.connect(connectString)
db_params = urllib.parse.quote_plus(connectString)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params), execution_options=dict(stream_results=True))
cursor = sqlserver.cursor()
cursor.close()

#
# To make it faster compared to one-to-one insertion of rows (see notes of Kiran Kumar Chilla)
#
@event.listens_for(engine,"before_cursor_execute")
def receive_before_cursor_execute(sqlserver, cursor, statement, connectString, context, executemany):
    if executemany:
        cursor.fast_executemany = True  # Change to False if utf-8 conversion failure in csv files, TODO: make parameter!
    return

#
# Functions
#
def printDatabaseVersion():
    cursor = sqlserver.cursor()
    cursor.execute("SELECT @@version;")
    print(cursor.fetchone())
    cursor.close()
    return

def printAllTables():
    cursor = sqlserver.cursor()
    cursor.execute("SELECT * FROM information_schema.tables")
    for row in cursor.fetchall():
        print(row)
    cursor.close()
    return

def getAllTables() -> list:
    tableList = []
    cursor = sqlserver.cursor()
    cursor.execute("SELECT * FROM information_schema.tables WHERE TABLE_NAME LIKE '%[_]'")
    for row in cursor.fetchall():
        tableList.append(row)
    cursor.close()
    return tableList

def tableExists(table) -> bool:
    cursor = sqlserver.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='dbo' AND table_name='{table}'")
    exists = cursor.fetchone()[0] == 1
    cursor.close()
    return exists

def countTableNumberRecords(table):
    cursor = sqlserver.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM [RA8552].[dbo].[{table}]")
    nrRows = cursor.fetchone()[0]
    cursor.close()
    return nrRows

def dropTable(table):
    cursor = sqlserver.cursor()    
    cursor.execute(f"DROP TABLE [RA8552].[dbo].[{table}]")
    cursor.close()
    return

def createChunkCsvFiles(table,file):
    chunknr = 0
    delimiter = fs.delimiterCsvFile(file)
    for chunk in pd.read_csv(file, delimiter=delimiter, engine='python', chunksize=CHUNKSIZE):
        chunk.to_csv(f"{table}.{str(chunknr)}", index=False)
        print(f"{table} imported: {str(len(chunk))}. Saved to {table}.{str(chunknr)}.")
        chunknr += 1
    return

def numberTableRecords(table):
    if not(tableExists(table)):
        return 0, 0, 0
    nrTableRecords = countTableNumberRecords(table)
    currentTableChunk = nrTableRecords // CHUNKSIZE
    currentTableResidue = nrTableRecords % CHUNKSIZE
    return nrTableRecords, currentTableChunk, currentTableResidue

# Convert a sav-file with whatever size by reading it in chunks
def createTableFromChunksOfSavFile(file) -> bool:
    if not(file.endswith('.sav') or file.endswith('.SAV')):
        print(f"File is not a SAV file, abort creation of table.")
        return False

    sqlDone = False

    try:
        # Check existence and contents of table
        table = fs.getTableNameFromFileName(file)
        print(f"Tablename {table}")
        nrTableRecords, currentTableChunk, currentTableResidue = numberTableRecords(table)

        # Compare table contents with SAV file
        start = (currentTableChunk + 1) * CHUNKSIZE + currentTableResidue
        df, meta = sav.read_sav(file, row_limit=CHUNKSIZE, row_offset=start)

        nrFileRecordsFromStart = df.shape[0]

        if nrFileRecordsFromStart == 0:
            print(f"{table}: Conversion already completed containing {currentTableChunk} chunks, and {currentTableResidue} remaining records.")
            return True

        if nrFileRecordsFromStart > currentTableResidue and nrFileRecordsFromStart == CHUNKSIZE and currentTableResidue != 0:
            print(f"{table}: Number of records in file from start = {nrFileRecordsFromStart} and residue records in table = {currentTableResidue}.")
            return True
        
        print(f"{table}: SAV data conversion continued from chunk {currentTableChunk} ...")

        # Convert dataframe df to sql, and read next chunk from sav-file
        while df.shape[0] > 0:
            df.to_sql(table, engine, if_exists='append', index=False, schema='dbo', method=None)
        
            print(f"{table}.{str(end)} records exported")
        
            start += df.shape[0]
            
            del df
            
            # Read next chunk, if at end, df.shape[0] == 0
            df, meta = sav.read_sav(file, row_limit=CHUNKSIZE, row_offset=start)
        
        sqlDone = True

        del df
        gc.collect()

    except Exception as ex:

        gc.collect()

        if(sqlDone == False):
            print(f"{table} export failed: {str(ex)}")
            return False

        return False

    return True

# Convert a sav-file with filesize < x GB by reading it in a dataframe at once, 
#   and convert this dataframe in small chunks to a sql table
def createTableFromSavFile(file) -> bool:

    if not(file.endswith('.sav') or file.endswith('.SAV')):
        print(f"File is not a SAV file, abort creation of table.")
        return False

    sqlDone = False

    try:
        # Check existence and contents of table
        table = fs.getTableNameFromFileName(file)
        print(f"Tablename {table}")
        nrTableRecords, currentTableChunk, currentTableResidue = numberTableRecords(table)

        # Compare table contents with SAV file
        df, meta = sav.read_sav(file)

        nrFileRecords = df.shape[0]
        nrFileChunks = nrFileRecords // CHUNKSIZE
        nrFileResidue = nrFileRecords % CHUNKSIZE

        if currentTableChunk == nrFileChunks and currentTableResidue == nrFileResidue:
            print(f"{table}: Conversion already completed containing {currentTableChunk} chunks, and {currentTableResidue} remaining records.")
            return True

        if currentTableResidue != 0:
            print(f"{table}: Number of records in table {nrTableRecords} and file {nrFileRecords} not equal.")
            return True
        
        print(f"{table}: SAV data conversion continued from chunk {currentTableChunk} ...")

        # Convert SAV to SQL in chunks
        chunk = -1
        for chunk in range(currentTableChunk, nrFileChunks):
            start = chunk * CHUNKSIZE
            end = start + CHUNKSIZE

            df[start: end].to_sql(table, engine, if_exists='append', index=False, schema='dbo', method=None)
            print(f"{table}.{str(end)} records exported")

        chunk += 1
        start = chunk * CHUNKSIZE
        end = start + nrFileResidue

        df[start: end].to_sql(table, engine, if_exists='append', index=False, schema='dbo', method=None)
        print(f"{table}.{nrFileResidue} remaining records exported")

        sqlDone = True

        del df
        gc.collect()

    except Exception as ex:

        #del df
        gc.collect()

        if(sqlDone == False):
            print(f"{table} export failed: {str(ex)}")
            return False

        return False

    return True

# General function independent of conversion route, depreciated
def createTableFromFile(file) -> bool:

    savDone = False
    sqlDone = False

    currentChunknr = 0
    nrTableRecords = -1    # table does not exists

    try:
        table = fs.getTableNameFromFileName(file)

        # Check if table exists and conversion completed
        if tableExists(table):
            nrTableRecords = countTableNumberRecords(table)

            # If not completed because number of records have integer factor of CHUNKSIZE (could be zero)
            if  nrTableRecords % CHUNKSIZE == 0:
                currentChunknr = nrTableRecords//CHUNKSIZE
                
                if file.endswith('.sav'):
                    print(f"{table} already exists: sav data conversion restarted and will replace existing table data ...")
                
                if file.endswith('.csv'):
                    print(f"{table} already exists: csv data conversion continued from chunk {currentChunknr} ...")

                #nrTableRecords = 0
                #dropTable(table)

            # If completed
            else:
                print(f"{table} already exists and conversion completed containing {nrTableRecords} records.")
                return True

        # CSV file processing, split in chunks first
        if file.endswith('.csv'):

            # Split in chunk files if table does not exists
            if nrTableRecords == -1:
                createChunkCsvFiles(table,file)
                savDone = True

            # Process the chunk files
            else:
                savDone = False
                sqlDone = False
                lastChunknr = fs.lastChunknrCsvFile(table)
                for chunknr in range(currentChunknr, lastChunknr + 1):
                    chunk = pd.read_csv(f"{table}.{str(chunknr)}", engine='python')
                    print(f"{table}.{str(chunknr)} imported: {str(chunk.shape[0])}")
                    savDone = True
                    sqlDone = False

                    chunk.to_sql(table, engine, if_exists='append', index=False, schema='dbo', method=None, chunksize=100000)
                    del chunk
                    gc.collect()
                    print(f"{table}.{str(chunknr)} exported")
                    sqlDone = True
                    savDone = False
                    os.remove(f"{table}.{chunknr}")

                    chunknr += 1

                savDone = True

        # SAV file processing
        else:
            df = pd.read_spss(file)

            print(f"{table} imported: {str(len(df))}")
            savDone = True

            df.to_sql(table, engine, if_exists='replace', index=False, schema='dbo', method=None) #, chunksize=100000)
            print(f"{table} exported")
            sqlDone = True

    except Exception as ex:

        if(savDone == False):
            print(f"{table} import failed: {str(ex)}")
            return False

        if(sqlDone == False):
            print(f"{table} export failed: {str(ex)}")
            return False

        return False

    return True

#
#  Main function
#
#  Go to source folder, activate conda environment with "conda activate environment.yml", start Python prompt 
#
#  First generate a file with all accessible files in the CBSMicroData disk.
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
#  Second step is the conversion from sav file to SQL table.
#
#  > import database as db
#  > db.runSavToSQL("accessible_files_proposal.txt")
#
def runSavToSQL(filelist):

    files = fs.readFiles(filelist)

    nrFilesSuccessFull = 0
    nrFilesFailed = 0
    nrFilesSkipped = 0

    for file in files:

        filesize = os.stat(file).st_size  # Measure the filesize in bytes

        if filesize > MAX_SAV_FILE_SIZE:
            print(f"{str(os.stat(file).st_size)} {file} too big, file needs manual conversion, skipping ...")
            nrFilesSkipped += 1
            continue

        print(f"{str(os.stat(file).st_size)} {file} processing ...")

        if createTableFromChunksOfSavFile(file): #createTableFromSavFile(file):
            nrFilesSuccessFull += 1

        else:
            nrFilesFailed += 1

        print(f"Files successfully converted:  {nrFilesSuccessFull}")
        print(f"Files failed when converted:   {nrFilesFailed}")
        print(f"Files skipped when converted:  {nrFilesSkipped}")

    return


