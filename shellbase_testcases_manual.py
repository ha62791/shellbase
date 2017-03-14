''' ========== setup before test ==========

Tables in HBase:

test
test2
test3
testtesttesttesttesttesttesttesttesttest4
testtesttesttesttesttesttesttesttesttest45
testtesttesttesttesttesttesttesttesttest456
testtesttesttesttesttesttesttesttesttest4567
testtesttesttesttesttesttesttesttesttest45678
testtesttesttesttesttesttesttesttesttest456789
testtesttesttesttesttesttesttesttesttest4567890
texting
'''


from shellbase import HBaseShell
shell = HBaseShell()


'''
====================
open
close
isOpen
===================='''

shell.isOpen()
'''
False
'''

shell.close()
'''
'''

shell.version()
'''
RuntimeError: Shell is not open
'''

shell.open('/usr/bin/hbase')
'''
2017-03-11 17:09:20,436 INFO  [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available

HBase Shell; enter 'help<RETURN>' for list of supported commands.
Type "exit<RETURN>" to leave the HBase Shell
Version 1.2.0-cdh5.8.0, rUnknown, Thu Jun 16 12:46:57 PDT 2016
'''

shell.isOpen()
'''
True
'''

shell.close()
'''
'''


'''
====================
setShellRespTimeout
===================='''

shell.setShellRespTimeout(None)
shell.setShellRespTimeout('123')
shell.setShellRespTimeout(9)
'''
ValueError: 'sec' should be an integer of at least 10
'''

shell.setShellRespTimeout(15)
'''
'''


'''
====================
version
===================='''

shell.version()
'''
'1.2.0-cdh5.8.0, rUnknown, Thu Jun 16 12:46:57 PDT 2016'
'''


'''
====================
whoami
===================='''

shell.whoami()
'''
{'auth': 'SIMPLE',
 'groups': ['cloudera', 'default', 'vboxsf'],
 'user': 'cloudera'}
'''


'''
====================
listTables
===================='''

# sudo service hbase-regionserver stop

shell.listTables('test')
'''
['test']
'''

# sudo service hbase-regionserver stop
# sudo service hbase-master stop

shell.listTables()
'''
RuntimeError: ERROR: Can't get master address from ZooKeeper; znode data == null
'''

# sudo service hbase-master start
# sudo service hbase-regionserver start

shell.listTables()
shell.listTables(None)
'''
['test',
 'test2',
 'test3',
 'testtesttesttesttesttesttesttesttesttest4',
 'testtesttesttesttesttesttesttesttesttest45',
 'testtesttesttesttesttesttesttesttesttest456',
 'testtesttesttesttesttesttesttesttesttest4567',
 'testtesttesttesttesttesttesttesttesttest45678',
 'testtesttesttesttesttesttesttesttesttest456789',
 'testtesttesttesttesttesttesttesttesttest4567890',
 'texting']
'''

shell.listTables(123)
'''
ValueError: 'filterRegex' should be of type 'str'
'''

shell.listTables('')
'''
[]
'''

shell.listTables('text.*')
'''
['texting']
'''

shell.listTables('test\d')
'''
['test2', 'test3']
'''

shell.listTables('.*test\d{3}')
'''
['testtesttesttesttesttesttesttesttesttest456']
'''

shell.listTables('.*test\d[5]')
'''
['testtesttesttesttesttesttesttesttesttest45']
'''

shell.listTables('.*test\d[5[6]')
'''
RuntimeError: ERROR: Unclosed character class near index 12
.*test\d[5[6]
            ^
'''



