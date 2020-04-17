#!/usr/bin/python
#-*-coding:utf-8-*-
import os
import termios
import sys
import threading
import time

import libtmux
from libtmux.exc import BadSessionName
import yamlordereddictloader
import yaml
from yaml.scanner import ScannerError
from yaml.parser import ParserError


class AuthenticationError(Exception):
    pass


class DatabaseError(Exception):
    pass


class HostError(Exception):
    pass


DBMS, CONNECTION_STRING, USER, PASSWORD, DB, HOST, \
    CONFIG_TABLE_INITIALIZATION, CONFIG_DBMS_STEPS, CLEAR_COMMAND, \
    TEST_NUM, TEST_COMMENT, NUMBER_OF_TRANSACTIONS, \
    TMUX_SERVER, TMUX_SESSION_NAME = (None,) * 14
SUPPORTED_DBMS = ['oracle', 'mysql', 'postgres', 'sqlserver']
KEEP_PRINTING_DOTS = False
YAML_FILE = None
AUTOCOMMIT_ON = None
AUTOCOMMIT_OFF = None
FILE_DESCRIPTOR = sys.stdin.fileno()
NORMAL_TERMINAL = termios.tcgetattr(FILE_DESCRIPTOR)


def validate_arguments():
    global DBMS, TEST_NUM, YAML_FILE

    if len(sys.argv) < 4:
        input('Argument error \n '
              'Make sure you provide <dbms>,  <testNum> and <yaml file path>')
        sys.exit(0)

    if str(sys.argv[1]) in SUPPORTED_DBMS:
        DBMS = sys.argv[1]
        TEST_NUM = sys.argv[2]
        YAML_FILE = sys.argv[3]
    else:
        input('Invalid DBMS name!\nSupported DBMSs are: ' + str(SUPPORTED_DBMS))
        sys.exit(0)


def parse_yaml(file_path):
    try:
        with open(file_path, 'r') as ymlfile:
            yaml_file = yaml.load(ymlfile, Loader=yamlordereddictloader.Loader)

        global USER, PASSWORD, DB, HOST, CONFIG_TABLE_INITIALIZATION, \
            CONFIG_DBMS_STEPS, TEST_COMMENT, NUMBER_OF_TRANSACTIONS

        USER = yaml_file[DBMS + '-config']['user']
        PASSWORD = yaml_file[DBMS + '-config']['password']
        DB = yaml_file[DBMS + '-config']['db']
        HOST = yaml_file[DBMS + '-config']['host']
        CONFIG_TABLE_INITIALIZATION = yaml_file['table-initialization']
        CONFIG_DBMS_STEPS = yaml_file[DBMS + '-tests'][TEST_NUM]
        TEST_COMMENT = find_comment(file_path, DBMS, TEST_NUM)

        transactions = []
        for steps in CONFIG_DBMS_STEPS:
            if steps[-2:] not in transactions:
                transactions.append(steps[-2:])

        NUMBER_OF_TRANSACTIONS = len(transactions)

        return yaml_file
    except KeyError as err:
        input('Error while parsing the yaml file - '
              'reason "%s"' % str(err) + ' does not exist')
    except IOError as err:
        print('Wrong yaml file path: \n'+str(err))
        sys.exit(0)
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. '
              'And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()
    except ParserError as err:
        print('\nError in tests/steps. '
              'Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()


def find_comment(file_path, dbms, test_num):
    try:
        with open(file_path, 'r') as ymlfile:
            line_num_dbms = 1000000
            commentLine = ''

            for line_num, line in enumerate(ymlfile):
                if dbms + '-tests:' in line:
                    line_num_dbms = line_num
                elif line_num > line_num_dbms:
                    if test_num in line:
                        commentLine = line.strip()
                        break

            return commentLine[commentLine.find('#'):None]
    except IOError as err:
        print('Wrong yaml file path: \n'+str(err))
        sys.exit()
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. '
              'And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()
    except ParserError as err:
        print('\nError in tests/steps. '
              'Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()


def find_comments(file_path, dbms):
    try:
        tests = []

        with open(file_path, 'r') as ymlfile:
            config = yaml.load(ymlfile, Loader=yamlordereddictloader.Loader)

        for test in config[dbms + '-tests']:
            tests.append(test + str(find_comment(file_path, dbms, test)))

        return tests
    except KeyError as err:
        print('Error while parsing the yaml file - '
              'reason "%s"' % str(err) + ' does not exist')
        sys.exit(0)
    except IOError as err:
        print('Wrong yaml file path: \n'+str(err))
        sys.exit()
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. '
              'And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()
    except ParserError as err:
        print('\nError in tests/steps. '
              'Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()


def prepare_connection():
    global CONNECTION_STRING, CLEAR_COMMAND
    global AUTOCOMMIT_ON, AUTOCOMMIT_OFF
    if is_dbms_running(DBMS):
        if DBMS == 'mysql':
            CONNECTION_STRING = 'mysql -u ' + USER + \
                                ' -p' + PASSWORD + \
                                ' -D ' + DB + \
                                ' -h ' + HOST
            CLEAR_COMMAND = 'system clear'
            AUTOCOMMIT_OFF = 'SET autocommit=0;;'
            AUTOCOMMIT_ON = 'SET autocommit=1;;'
        elif DBMS == 'postgres':
            CONNECTION_STRING = "export PGPASSWORD='" + PASSWORD + "'; " + \
                                ' psql' + \
                                ' -h ' + HOST + \
                                ' -d ' + DB + \
                                ' -U ' + USER
            CLEAR_COMMAND = '\! clear'
            AUTOCOMMIT_OFF = '\set AUTOCOMMIT off'
            AUTOCOMMIT_ON = '\set AUTOCOMMIT on'
        elif DBMS == 'sqlserver':
            CONNECTION_STRING = 'sqlcmd -U ' + USER + \
                                ' -P' + PASSWORD + \
                                ' -d ' + DB + \
                                ' -S ' + HOST
            CLEAR_COMMAND = ':RESET'
            AUTOCOMMIT_OFF = 'SET IMPLICIT_TRANSACTIONS ON'
            AUTOCOMMIT_ON = 'SET IMPLICIT_TRANSACTIONS OFF'
        elif DBMS == 'oracle':
            CONNECTION_STRING = str('sqlplus ' +
                                    USER + '/' +
                                    PASSWORD + '@' +
                                    HOST + '/' +
                                    DB)
            CLEAR_COMMAND = 'clear screen'
            AUTOCOMMIT_OFF = 'set autocommit off;;'
            AUTOCOMMIT_ON = 'set autocommit on;;'
    else:
        input(DBMS + ' is not running')

    return CONNECTION_STRING


def is_dbms_running(dbms):
    dbms_service = None
    if "debian" in str(os.uname()):
        if dbms == 'oracle':
            dbms_service = 'oracle-xe'
        elif dbms == 'postgres':
            dbms_service = 'postgresql'
        elif dbms == 'sqlserver':
            dbms_service = 'mssql-server'
        elif dbms == 'mysql':
            dbms_service = 'mysql'
        if os.system(str('systemctl is-active --quiet ' + dbms_service)) == 0:
            return True
        else:
            return False

    return True


def check_connection(connection_result, connection_string):
    # concatenate the error messages from a list to a string
    connection_result_string = ' '.join([str(elem) for elem in connection_result])

    database_errors = ['Unknown database',
                       'FATAL:  database',
                       'Cannot open database',
                       'ORA-12514']
    for db_error in database_errors:
        if db_error in connection_result_string:
            raise DatabaseError(connection_string)

    host_errors = ['Unknown MySQL server host',
                   'could not translate host name',
                   'Login timeout expired',
                   'could not connect',
                   'could not resolve',
                   'ORA-12541']
    for host_error in host_errors:
        if host_error in connection_result_string:
            raise HostError(connection_string)

    authentication_errors = ['Access denied',
                             'authentication failed',
                             'psql: warning: extra command',
                             'Login failed',
                             'invalid username/password']
    for auth_error in authentication_errors:
        if auth_error in connection_result_string:
            raise AuthenticationError(connection_string)

    success = ['1>', '=#', ']>', '>']
    for db_success in success:
        if db_success in connection_result_string:
            return True
    else:
        return False


def create_tmux_window_and_panes():
    try:
        global TMUX_SERVER, TMUX_SESSION_NAME
        TMUX_SERVER = libtmux.Server()
        TMUX_SESSION_NAME = (DBMS.upper() + ' -- ' + TEST_COMMENT)
        session = TMUX_SERVER.new_session(session_name=TMUX_SESSION_NAME,
                                          kill_session=True,
                                          attach=False)
        window = session.new_window(attach=True,
                                    window_name=TMUX_SESSION_NAME)

        tmux_panes = []

        pane1 = window.attached_pane
        pane1.reset()
        tmux_panes.append(pane1)

        if NUMBER_OF_TRANSACTIONS >= 2:
            pane2 = window.split_window(vertical=False)
            pane2.reset()
            tmux_panes.append(pane2)

        if NUMBER_OF_TRANSACTIONS >= 3:
            pane3 = window.split_window(vertical=False)
            pane3.reset()
            tmux_panes.append(pane3)

        window.select_layout('even-horizontal')

        return tmux_panes
    except BadSessionName as err:
        print('Probably a comment in a test contains an invalid character '
              'like a colon (:) or a period (.)\n')
        time.sleep(0.3)
        print_dots(False)
        print('Error:' + str(err))
        input('\n\nPress Enter to exit...')
        sys.exit(0)
    except Exception as err:
        time.sleep(0.3)
        print_dots(False)
        print('Error:' + str(err))
        input('\n\nPress Enter to exit...')
        sys.exit(0)


def initiate_connection(pane):
    pane.send_keys(CONNECTION_STRING)

    start_time = time.time()

    while True:
        if time.time() - start_time > 16:
            print_dots(False)
            print ("\n15 seconds have passed, probably the host is unreachable.\n")
            print ("Check your yaml configuration file "
                   "and make sure the service is running\n")
            enter_pressed = input('\nPress Enter to exit..')
            if enter_pressed == "":
                sys.exit(1)

        time.sleep(0.5)
        output = pane.capture_pane()
        connected = check_connection(output, CONNECTION_STRING)
        if not connected:
            continue
        else:
            break


def initiate_panes(panes):
    try:
        print('Connecting to ' + DBMS)

        initiate_connection(panes[0])

        # terminate any left over transactions
        panes[0].send_keys(AUTOCOMMIT_ON)

        # table re-initialization commands in the yaml file
        for create_table_instructions in CONFIG_TABLE_INITIALIZATION:
            if DBMS == 'sqlserver':
                if create_table_instructions == 'COMMIT;;':
                    continue

                panes[0].send_keys('GO')
                time.sleep(0.3)

            panes[0].send_keys(create_table_instructions)

        if DBMS == 'sqlserver':
            time.sleep(1)
            panes[0].send_keys('ALTER DATABASE ' + DB +
                               ' SET READ_COMMITTED_SNAPSHOT ON;;')
            panes[0].send_keys('GO')
            panes[0].send_keys('ALTER DATABASE ' + DB +
                               ' SET ALLOW_SNAPSHOT_ISOLATION ON;;')
            panes[0].send_keys('GO')

        panes[0].send_keys(AUTOCOMMIT_OFF)

        # so as to show only the Transaction relevant data in the console
        panes[0].send_keys(CLEAR_COMMAND)
        panes[0].send_keys('')

        iter_panes = iter(panes)

        # skip the first pane which is being handled above
        next(iter_panes)

        for pane in iter_panes:
            # pane.send_keys(connection_string)
            # time.sleep(2)
            initiate_connection(pane)
            pane.send_keys(AUTOCOMMIT_OFF)
            pane.send_keys(CLEAR_COMMAND)
            pane.send_keys('')

    except HostError as err:
        print_dots(False)
        input('\nUnknown host:\n' + str(err) +
              '\n\nPress Enter to exit..')
    except DatabaseError as err:
        print_dots(False)
        input('\nDatabase might not exist:\n' + str(err) +
              '\n\nPress Enter to exit..')
    except AuthenticationError as err:
        print_dots(False)
        input('\nAuthentication Error trying to connect:\n' +
              str(err) + '\n\nPress Enter to exit..')


def execute_steps(tmux_panes):
    transaction = 'T1'
    pane = None
    print ('\nExecuting test ' + TEST_COMMENT)
    for steps in CONFIG_DBMS_STEPS:
        current_transaction = steps[-2:]    # last two characters (e.g. 'T2' from 'step2_T2')
        if transaction != current_transaction:
            time.sleep(1)   # wait 1 second after switching transaction

        if steps[-2:] == 'T1':
            pane = tmux_panes[0]    # use the proper pane, depending on the Transaction
        elif steps[-2:] == 'T2':
            pane = tmux_panes[-1]   # the pervious to last element of the list
        elif steps[-2:] == 'T3':
            pane = tmux_panes[1]

        for transaction_steps in CONFIG_DBMS_STEPS[steps]:
            pane.send_keys(transaction_steps)   # execute the transaction's steps
            if DBMS == 'sqlserver':
                pane.send_keys('GO')
                time.sleep(0.1)

        transaction = current_transaction

    tmux_panes[0].select_pane()


def run_tmux():
    try:
        tmux_panes = create_tmux_window_and_panes()

        initiate_panes(tmux_panes)

        execute_steps(tmux_panes)

        print_dots(False)
        time.sleep(0.5)

        # show tmux console
        TMUX_SERVER.attach_session(target_session=TMUX_SESSION_NAME)
    except TypeError as err:
        print_dots(False)
        input('\nYou probably have a formatting error in the yaml file'
              '\nPlease check the syntax, close this window and re-run the test\n'
              '\nError: ' + str(err))


def print_dots(keep_printing):
    global KEEP_PRINTING_DOTS
    KEEP_PRINTING_DOTS = keep_printing

    while KEEP_PRINTING_DOTS:
        time.sleep(0.5)
        sys.stdout.write('.')
        sys.stdout.flush()

    if not keep_printing:
        KEEP_PRINTING_DOTS = False


def hide_user_input(hide):
    no_input_terminal = termios.tcgetattr(FILE_DESCRIPTOR)
    no_input_terminal[3] = no_input_terminal[3] & ~termios.ECHO  # lflags
    if hide:
        termios.tcsetattr(FILE_DESCRIPTOR, termios.TCSADRAIN, no_input_terminal)
    else:
        termios.tcsetattr(FILE_DESCRIPTOR, termios.TCSADRAIN, NORMAL_TERMINAL)


def main():
    hide_user_input(True)

    validate_arguments()

    parse_yaml(YAML_FILE)

    prepare_connection()

    thread_tmux = threading.Thread(target=run_tmux)
    thread_tmux.start()

    time.sleep(0.3)
    print_dots(True)

    thread_tmux.join()

if __name__ == "__main__":
    main()
