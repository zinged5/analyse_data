
import datetime
from os import path
import pandas as pd
from python311 import os
import logging
from dataclasses import dataclass
import itertools
@dataclass
class start_data_analysis:
    source_folder: str
    maintablenameprefix: str
    maincolumnname: str
    secondarycolumnname: str

    # import files and merge into one df
    def importdata(self):
        source_directory =self.source_folder
        source_files = os.listdir(source_directory)
        dfs = {}
        try:
            for file in source_files:
                if file.rfind('csv') > 0:
                    filename = file.replace('.csv', '').replace(' ', '')
                    dfname = pd.read_csv(f'{source_directory}{file}', infer_datetime_format='yyyyMMdd')
                    dfname.columns = map(str.lower, dfname.columns)
                    dfname['filename'] = filename
                    dfs[filename] = dfname.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
            return dfs
        except:
            logging.log(30, 'failed to import files from source directory, please check and try again')

    def analysecount(self):
        source_folder=self.source_folder
        try:
            dfs = self.importdata()
            dfcounts = pd.DataFrame(columns=['filename', 'count'])
            for df in dfs:
                dfcounts = dfcounts.append({'filename': df, 'count': dfs[df].shape[0]}, ignore_index=True)
            return dfcounts
        except:
            logging.log(30, 'failed to analyse count, check if files were imported correctly from source directory')
        return

    def lookup_matches(self):
        maincol=self.maincolumnname
        secondarycol=self.secondarycolumnname
        maintables=self.maintablenameprefix
        try:
            dfs = self.importdata()
            maintable = list(itertools.chain.from_iterable([dfs[i][maincol].values for i in dfs if i.startswith(maintables)]))
            secondarytable = list(
                itertools.chain.from_iterable([dfs[i][secondarycol].values for i in dfs if i.startswith(maintables) == False]))
            matched_values = set(secondarytable).intersection(set(maintable))
            return matched_values
        except:
            logging.log(30, 'failed to lookup matches please check the files for errors')
        return -1


    def data_validation_passed(self):
        maincol = self.maincolumnname
        secondarycol = self.secondarycolumnname
        maintables=self.maintablenameprefix
        try:
            matches = self.lookup_matches()
            counts=self.analysecount()
            count = 0
            for i, v in counts.iterrows():
                if maintables not in v['filename']:
                    count = count + v['count']
            if count != len(matches):
                logging.log(30, f'total number of records to match {count} while number of matches are {len(matches)}')
                return False
            else:
                return True
        except:
            logging.log(30,
                        'failed to validate data, please check your files for corruption or recheck column names provided. ')

if __name__ == '__main__':
    source_folder=input('Please enter source directory for the files: ').lower()
    maintablenameprefix = input('Enter the prefix file name for where you would search the values in: ').lower()
    maincolumnname = input('Enter the column name from the maintable where you could lookup the values: ').lower()
    secondarycolumnname = input('Enter the column name from the secondary files which you would search from to see if there is a match in the main table: ').lower()

    analysis=start_data_analysis(source_folder,maintablenameprefix,maincolumnname,secondarycolumnname)
    print(f'..starting data analysis at {datetime.datetime.now()}')
    counts=analysis.analysecount()
    print(f' count analysis .. {counts}')
    found_matches=analysis.lookup_matches()
    print(f' here are the matches between {maincolumnname}  and {secondarycolumnname}  : {found_matches}')
    data_validation= analysis.data_validation_passed()
    print(f'data validation results.. {data_validation}')
    print(f'..ended data analysis at {datetime.datetime.now()}')