#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
import time
from mvcc_runner import find_comment
from mvcc_runner import find_comments
from mvcc_runner import is_dbms_running

try:
    import pick
except ImportError as err:

    if 'pick' in str(err):
        print "Trying to Install required module: pick\n"
        os.system('python -m pip install pick')
        print('\nScreen will now clear')
        time.sleep(3)
        os.system('clear')

from pick import Picker


##############################
YAML_FILE = "./mvcc_tests.yml"
##############################


if len(sys.argv) < 2:
    print('Argument error \n Make sure you provide <dbms> and <testNum>')
    sys.exit(1)

dbms = sys.argv[1]
test_num = ''

if len(sys.argv) == 3:
    test_num = sys.argv[2]

if not is_dbms_running(dbms):
    print(dbms + ' is not running')
    sys.exit(0)

if len(sys.argv) == 2:
    dbms = sys.argv[1]
    comments = find_comments(YAML_FILE, dbms)
    print('\nAvaiable tests for ' + dbms + ': \n')
    print("\t" + "\n\t".join(comments) + '\n')
    while True:
        try:
            choice = int(raw_input('Please type the *number* of your choice and press Enter..\n'))
        except ValueError:
            print("Accepting only numeric input\n")
            continue
        if not choice <= len(comments) or choice <= 0:
            print("There is no such test number!\n")
            continue
        else:
            test_num = 'test' + str(choice)
            break


# if len(sys.argv) == 2:
#     comments = find_comments(YAML_FILE, dbms)
#     title = 'Choose a test:'
#     picker = Picker(comments, title)
#     picker.indicator = '==>'
#     option, index = picker.start()
#     test_num = 'test' + str(index+1)



test_comment = find_comment(YAML_FILE, dbms, test_num)

subprocess.call(['x-terminal-emulator', '-title', dbms.upper() + ' - ' + test_num.upper() + ' - ' + test_comment, '-geometry', '150x52', '-e', 'python mvcc_runner.py ' + dbms + ' ' + test_num + ' ' + YAML_FILE])
