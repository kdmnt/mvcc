#!/usr/bin/python
"""
Opens a tmux session in the current terminal
and executes the test scenario's steps.

To run:
$ python mvcc_runner.py <dbms> <test_num> <yaml_file_path>
e.g.( python mvcc_runner.py oracle test1 "./mvcc_tests.yml" )
------------------------------------------------------------------------------
Author: Konstantinos Diamantidis - March 2020
------------------------------------------------------------------------------
"""
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
    CONFIG_TABLE_INITIALIZATION, CONFIG_DBMS_STEPS, \
    CLEAR_COMMAND, AUTOCOMMIT_ON, AUTOCOMMIT_OFF, \
    YAML_FILE, TEST_NUM, TEST_COMMENT, NUMBER_OF_TRANSACTIONS, \
    TMUX_SERVER, TMUX_SESSION_NAME = (None,) * 17
SUPPORTED_DBMS = ['oracle', 'mysql', 'postgres', 'sqlserver']
KEEP_PRINTING_DOTS = False
FILE_DESCRIPTOR = sys.stdin.fileno()
NORMAL_TERMINAL = termios.tcgetattr(FILE_DESCRIPTOR)


def validate_arguments():
    """Validates that the arguments provided are of the correct number
    and that the dbms argument provided is one of the supported ones.
    Test number and yaml file path validation will occur in the parse_yaml function
    """
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
    """
    Will load the provided yaml file.
    Will set some global variables based on the parsed yaml file.

    :param file_path: path of the yaml file
    :return: the loaded yaml file
    """
    try:
        with open(file_path, 'r') as ymlfile:
            # yamlordereddictloader is needed so as to
            # be sure that tests and steps are parsed sorted
            # so that they get printed in the terminal window sorted
            yaml_file = yaml.load(ymlfile, Loader=yamlordereddictloader.Loader)

        global USER, PASSWORD, DB, HOST, \
                CONFIG_TABLE_INITIALIZATION, CONFIG_DBMS_STEPS, \
                TEST_COMMENT, NUMBER_OF_TRANSACTIONS

        USER = yaml_file[DBMS + '-config']['user']
        PASSWORD = yaml_file[DBMS + '-config']['password']
        DB = yaml_file[DBMS + '-config']['db']
        HOST = yaml_file[DBMS + '-config']['host']
        CONFIG_TABLE_INITIALIZATION = yaml_file['table-initialization']
        CONFIG_DBMS_STEPS = yaml_file[DBMS + '-tests'][TEST_NUM]
        TEST_COMMENT = find_comment(file_path, DBMS, TEST_NUM)

        transactions = []
        for steps in CONFIG_DBMS_STEPS:
            # steps[-2:] will be e.g.: 'T1' from step3_T1
            if steps[-2:] not in transactions:
                # will fill the list with the unique T1, T2, T3
                transactions.append(steps[-2:])
        # will be used for splitting the tmux session
        # into the appropriate panes
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
        sys.exit(0)
    except ParserError as err:
        print('\nError in tests/steps. '
              'Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit(0)


def find_comment(file_path, dbms, test_num):
    """
    Will return the comment next to the provided test number.
    e.g.: from "test4:    # Anomaly|Lost Update - Isolation|Serializable"
    it will return "# Anomaly|Lost Update - Isolation|Serializable"

    This will be used in the terminal window title.

    :param file_path: yaml file path
    :param dbms: 'oracle' | 'mysql' | 'postgres' | 'sqlserver'
    :param test_num: e.g. 'test2'
    :return: comment next to test number
    """
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
        sys.exit(0)
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. '
              'And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit(0)
    except ParserError as err:
        print('\nError in tests/steps. '
              'Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit(0)


def find_comments(file_path, dbms):
    """
    Will return a list of the comments next to each test scenario.

    These will be used in the menu from which the user
    can select the test scenario to be executed.

    :param file_path: yaml file path
    :param dbms: 'oracle' | 'mysql' | 'postgres' | 'sqlserver'
    :return: list of comments next to test scenarios
    """
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
        sys.exit(0)
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. '
              'And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit(0)
    except ParserError as err:
        print('\nError in tests/steps. '
              'Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit(0)


def prepare_connection():
    """Populates global variables with values based on the selected DBMS."""
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
    """Checks if selected DBMS service is running"""
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
    """Checks connection output and determines
    if connection is established or if an error occurred"""
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

    # these are the success prompts in
    # sqlserver | postgres | mysql | oracle
    success = ['1>', '=#', ']>', '>']
    for db_success in success:
        if db_success in connection_result_string:
            return True
    else:
        return False


def create_tmux_window_and_panes():
    """
    Initiates the tmux server, the window session
    and creates the appropriate number of panes (up to 3).

    :return: tmux panes objects in a list
    """
    try:
        global TMUX_SERVER, TMUX_SESSION_NAME
        TMUX_SERVER = libtmux.Server()
        TMUX_SESSION_NAME = ('Switch Panes = (CTRL + B) + Arrows, '
                             'Scroll Mode = (CTRL+ B) + "[", '
                             'Quit Scroll Mode = "q" |')
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
    """
    Initiates the dbms connection in the provided pane.

    :param pane: tmux pane in which the dbms connection will take place
    :return:
    """
    pane.send_keys(CONNECTION_STRING)

    start_time = time.time()

    # try to connect until connection is established
    # will try for 15 seconds, then execution will stop
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
        # capture the attempt output
        # so as to check if connection is successful
        # or if an error occurred
        output = pane.capture_pane()
        connected = check_connection(output, CONNECTION_STRING)
        if not connected:
            continue
        else:
            break


def initiate_panes(panes):
    """
    Initializes the dbms connections and
    re-initializes the tables as per the
    table initialization section in the yaml file.

    :param panes: tmux panes
    :return: None
    """
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

        # clear so as to show only the Transaction relevant data in the console
        panes[0].send_keys(CLEAR_COMMAND)
        panes[0].send_keys('')

        iter_panes = iter(panes)

        # skip the first pane which is being handled above
        next(iter_panes)

        for pane in iter_panes:
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
    """
    Executes the steps from the selected dbms's
    test scenario in the appropriate pane.

    :param tmux_panes: all the tmux panes
    :return: None
    """
    transaction = 'T1'
    pane = None
    print ('\nExecuting test ' + TEST_COMMENT)
    for steps in CONFIG_DBMS_STEPS:
        # last two characters (e.g. 'T2' from 'step2_T2')
        current_transaction = steps[-2:]
        if transaction != current_transaction:
            # wait 1 second after switching transaction
            time.sleep(1)
        if steps[-2:] == 'T1':
            # use the proper pane, depending on the Transaction
            pane = tmux_panes[0]
        elif steps[-2:] == 'T2':
            # the previous to last element of the list
            pane = tmux_panes[-1]
        elif steps[-2:] == 'T3':
            pane = tmux_panes[1]

        for transaction_steps in CONFIG_DBMS_STEPS[steps]:
            # execute the transaction's steps
            pane.send_keys(transaction_steps)
            if DBMS == 'sqlserver':
                pane.send_keys('GO')
                time.sleep(0.1)

        transaction = current_transaction

    tmux_panes[0].select_pane()


def run_tmux():
    """The "main" function of the tmux feature."""
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
    """
    Prints dot's in the terminal window.
    Used while waiting for the dbms connection and
    during the test steps' execution.

    :param keep_printing: True/False
    :return: None
    """
    global KEEP_PRINTING_DOTS
    KEEP_PRINTING_DOTS = keep_printing

    while KEEP_PRINTING_DOTS:
        time.sleep(0.5)
        sys.stdout.write('.')
        sys.stdout.flush()

    if not keep_printing:
        KEEP_PRINTING_DOTS = False


def hide_user_input(hide):
    """
    Does not show keystrokes in the temrinal.
    Used while waiting for the dbms connection and
    during the test steps' execution, so as to have
    a cleaner output.

    :param hide: True/False
    :return: None
    """
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
    hide_user_input(False)


if __name__ == "__main__":
    main()
