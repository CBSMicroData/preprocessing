## CBS Micro Data preprocessing pipeline

This repository contains software in Python to search sav-files based on given categories found in the CBS MicroData.
The software steps through a list of sav-files, and converts the data into a corresponding SQL table, one-by-one.

## Conversion of sav files to SQL tables

The conversion of sav files to SQL tables is done with the following steps. 

Install the Python files and Python packages listed in the environment.yml file.

Go to the source folder, activate conda environment with "conda activate environment.yml", start the Python prompt.

Change the rootfolder in the source file file.py with the Micro Data categories you wish to explore!

Generate a file with all accessible files in the CBSMicroData disk.

```
> import files as fs
> fs.createFileOfExtensionFiles(fs.rootfolders, '.sav')
```

This produces accessible_files_proposal.txt and none_accessible_files_proposal.txt

```
> fs.createFileOfTableNames(fs.readFiles("accessible_files_proposal.txt"))
```

This produces tablenames_proposal.txt file with size of the files included, to check the size and table name.

```
> fs.createMetaDataOfSavFiles(fs.readFiles("accessible_files_proposal.txt"))
```

This produces meta data of sav files found in "accessible_files_proposal.txt".

Start the conversion from sav file to SQL table.

```
> import database as db
> db.runSavToSQL("accessible_files_proposal.txt")
```
