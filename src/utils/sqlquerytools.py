# Reusable functions for SQL Query with pyodbc
# LICENSE: Apache License - Version 2.0, January 2004 - http://www.apache.org/licenses/
# Version: 1.0.0
# Author: Z WANG
# Date: 06/Mar/2022

# setting the path
import sys

sys.path.append(r'..\..\configure')

import conf
import pyodbc
import pandas as pd

from IPython.core.display_functions import display


class SqlQueryResult(object):
    ''' SQL Query Class
    Attributes:
        __init__ : connect to the server, when instance queryer has been created.
        connect_to_server() : function that returns a connection class to server.
        disconnect() : function that closes the connection.
        query(sql_code) : query function which shows the query result in jupyter notebook.
        result(*args) : When argument is empty, returns the last query results in Pandas Dataframe.
                        When argument is SQL query code, returns current query results in Pandas Dataframe.
    '''
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
        ''' query database
            Args:
                sql_code : str , SQL query code.
            Returns:
                display the query results
        '''
        # Send the SQL Query, and record response as a Pandas dataframe then show in jupyter.
        if self.connect_flag == 0:
            raise Exception('Error: No Connection. Use SqlQueryResult.connect_to_server()')
        else:
            temp = pd.read_sql_query(sql_code, self.connect)
            self.query_result = pd.DataFrame(temp)
            display(self.query_result)

    def result(self, *args):
        ''' query database
            Args:
                sql_code : str , SQL query code. / empty
            Returns:
                query_result
        '''
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

'''
Example:
# Config the server first.
from sqlquerytools import SqlQueryResult
# Create the instance and initialization.
z_sql = SqlQueryResult()
# Query
z_sql.query(sql_code)
# Results return in Pandas DataFrame.
q1 = z_sql.result()
q2 = z_sql.result(sql_code2)
# Connection
z_sql.disconnect()
z_sql.connect_to_server()
'''

# print(Query.__dict__)

