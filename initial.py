
import pyodbc

# SQL Details
sql_driver_str = '******'
server_str = '******'
database_str = '******'
user_str = '******'
pass_str = '******'
# Establishing connection to SQL database
print('*** Connecting to SQL Server... ***')
conn = pyodbc.connect('DRIVER=%s;'
                      'SERVER=%s;'
                      'DATABASE=%s;'
                      'UID=%s;'
                      'PWD=%s' % (sql_driver_str, server_str, database_str, user_str, pass_str))
print('*** Connection Established. ***')



conn.close()
print('*** Connection Closed. ***')


print('project AFL started')