# Reusable functions for SQL Query with pyodbc

# setting the path
import sys

sys.path.append(r'..\..\configure')

import conf
import pyodbc
import pandas as pd

from IPython.core.display_functions import display


class SqlQueryResult(object):

    def __init__(self):
        self.connect_flag = 0
        self.connect = 'Initialization'
        self.connect_to_server()
        self.query_result = 'Initialization'
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

    def connect_to_server(self):
        # check the connection flag.
        if self.connect_flag == 0:
            print('*** Connecting to SQL Server... ***')
            self.connect = pyodbc.connect(str(conf.configs['db']))
            self.connect_flag = 1
            print('*** Connection Established. ***')
        else:
            print('*** Connection Has Already Been Established. ***')

    def disconnect(self):
        self.connect.close()
        self.connect_flag = 0
        print('*** Connection Closed. ***')

    def query(self, sql_code):
        # Send the SQL Query, and record response as a Pandas dataframe then show in jupyter.
        if self.connect_flag == 0:
            raise Exception('Error: No Connection. Use SqlQueryResult.connect_to_server()')
        else:
            temp = pd.read_sql_query(sql_code, self.connect)
            self.query_result = pd.DataFrame(temp)
            display(self.query_result)

    def result(self, *args):
        if not args:
            if isinstance(self.query_result, str):
                raise Exception('Error: No Query. Use SqlQueryResult.query(code) or SqlQueryResult.result(code)')
            else:
                return self.query_result
        else:
            # Send the SQL Query, and return response as a Pandas dataframe.
            if self.connect_flag == 0:
                raise Exception('Error: No Connection. Use SqlQueryResult.connect_to_server()')
            else:
                temp = pd.read_sql_query(args[0], self.connect)
                self.query_result = pd.DataFrame(temp)
                return self.query_result

# print(Query.__dict__)
