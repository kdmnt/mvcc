#!/usr/bin/python
#-*-coding:utf-8-*-


import os
import termios
import sys
import threading
import time

import libtmux
from libtmux.exc import BadSessionName
import yaml
import yamlordereddictloader
from yaml.scanner import ScannerError
from yaml.parser import ParserError

class AuthenticationError(Exception):
    pass

class DatabaseError(Exception):
    pass

class HostError(Exception):
    pass


dbms, test_num, user, password, db, host, cfg_table_initialization, cfg_dbms_steps, \
    connection_string, clear, server, session_name, test_comment, number_of_transactions = (None,) * 14
supported_dbms = ['oracle' , 'mysql', 'postgres', 'sqlserver']
keep_printing_dots = False
YAML_FILE = None
autocommit_on = None
autocommit_off = None
fd = sys.stdin.fileno()
normal_terminal = termios.tcgetattr(fd)


def validate_arguments():
    global dbms, test_num, YAML_FILE

    if len(sys.argv) < 4:
        input('Argument error \n Make sure you provide <dbms>,  <testNum> and <yaml file path>')
        sys.exit(0)

    if str(sys.argv[1]) in supported_dbms:
        dbms = sys.argv[1]
        test_num = sys.argv[2]
        YAML_FILE = sys.argv[3]
    else:
        input('Invalid DBMS name!\nSupported DBMSs are: ' + str(supported_dbms))
        sys.exit(0)


def parse_yaml(filePath):
    try:
        with open(filePath, 'r') as ymlfile:         
            global cfg
            cfg = yaml.load(ymlfile, Loader=yamlordereddictloader.Loader)

        global user, password, db, host, cfg_table_initialization, \
            cfg_dbms_steps, test_comment, number_of_transactions

        user = cfg[dbms + '-config']['user']
        password = cfg[dbms + '-config']['password']
        db = cfg[dbms + '-config']['db']
        host = cfg[dbms + '-config']['host']
        cfg_table_initialization = cfg['table-initialization']
        cfg_dbms_steps = cfg[dbms + '-tests'][test_num]
        test_comment = find_comment(filePath, dbms, test_num)

        transactions = []
        for steps in cfg_dbms_steps:
            if steps[-2:] not in transactions:
                transactions.append(steps[-2:])

        number_of_transactions = len(transactions)

        return cfg
    except KeyError as err:
        input('Error while parsing the yaml file - reason "%s"' % str(err) + ' does not exist')
    except IOError as err:
        print('Wrong yaml file path: \n'+str(err))
        sys.exit(0)
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()
    except ParserError as err:
        print('\nError in tests/steps. Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()


def find_comment(filePath, dbms, test_num):
    try:
        with open(filePath, 'r') as ymlfile:
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
        print('\nError in whitespace, no tabs should be used. And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()
    except ParserError as err:
        print('\nError in tests/steps. Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()


def find_comments(filePath, dbms):
    try:
        tests = []

        with open(filePath, 'r') as ymlfile:
            config = yaml.load(ymlfile, Loader=yamlordereddictloader.Loader)

        for test in config[dbms + '-tests']:
            tests.append(test + str(find_comment(filePath, dbms, test)))

        return tests
    except KeyError as err:
        print('Error while parsing the yaml file - reason "%s"' % str(err) + ' does not exist')
        sys.exit(0)
    except IOError as err:
        print('Wrong yaml file path: \n'+str(err))
        sys.exit()
    except ScannerError as err:
        print('\nError in whitespace, no tabs should be used. And no whitespace is allowed at the end of a line')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()
    except ParserError as err:
        print('\nError in tests/steps. Make sure they are properly aligned')
        print('\n\nFollowing the error message:\n\n' + str(err))
        sys.exit()


def prepare_connection():
    global connection_string, clear
    global autocommit_on, autocommit_off
    if is_dbms_running(dbms):
        if dbms == 'mysql':
            connection_string = 'mysql -u ' + user + ' -p' + password + ' -D ' + db + ' -h ' + host
            clear = 'system clear'
            autocommit_off = 'SET autocommit=0;;'
            autocommit_on = 'SET autocommit=1;;'
        elif dbms == 'postgres':
            connection_string = "export PGPASSWORD='" + password + "'; " + ' psql' + ' -h ' + host + ' -d ' + db + ' -U ' + user
            clear = '\! clear'
            autocommit_off = '\set AUTOCOMMIT off'
            autocommit_on = '\set AUTOCOMMIT on'
        elif dbms == 'sqlserver':
            connection_string = 'sqlcmd -U ' + user + ' -P' + password + ' -d ' + db + ' -S ' + host
            clear = ':RESET'
            autocommit_off = 'SET IMPLICIT_TRANSACTIONS ON'
            autocommit_on = 'SET IMPLICIT_TRANSACTIONS OFF'
        elif dbms == 'oracle':
            connection_string = str('sqlplus ' + user + '/' + password + '@' + host + '/' + db)
            clear = 'clear screen'
            autocommit_off = 'set autocommit off;;'
            autocommit_on = 'set autocommit on;;'
    else:
        input(dbms + ' is not running')

    return connection_string


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
    connection_result_string = ' '.join([str(elem) for elem in connection_result]) #concatenate the error messages from a list to a string

    database_errors = ['Unknown database', 'FATAL:  database', 'Cannot open database', 'ORA-12514']
    for db_error in database_errors:
        if db_error in connection_result_string:
            raise DatabaseError(connection_string)

    host_errors = ['Unknown MySQL server host', 'could not translate host name', 'Login timeout expired', 'could not connect', 'could not resolve', 'ORA-12541']
    for host_error in host_errors:
        if host_error in connection_result_string:
            raise HostError(connection_string)

    authentication_errors = ['Access denied', 'authentication failed', 'psql: warning: extra command' , 'Login failed', 'invalid username/password']
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
        global server, session_name
        server = libtmux.Server()
        session_name = (dbms.upper() + ' -- ' + test_comment)
        session = server.new_session(session_name=session_name, kill_session=True, attach=False)
        window = session.new_window(attach=True, window_name=session_name)

        tmux_panes = []

        pane1 = window.attached_pane
        pane1.reset()
        tmux_panes.append(pane1)

        if number_of_transactions >= 2:
            pane2 = window.split_window(vertical=False)
            pane2.reset()
            tmux_panes.append(pane2)

        if number_of_transactions >= 3:
            pane3 = window.split_window(vertical=False)
            pane3.reset()
            tmux_panes.append(pane3)

        window.select_layout('even-horizontal')

        return tmux_panes
    except BadSessionName as e:
        print('Probably a comment in a test contains an invalid character like a colon (:) or a period (.)\n')
        time.sleep(0.3)
        print_dots(False)
        print('Error:' + str(e))
        input('\n\nPress Enter to exit...')
        sys.exit(0)
    except Exception as e:
        time.sleep(0.3)
        print_dots(False)
        print('Error:' + str(e))
        input('\n\nPress Enter to exit...')
        sys.exit(0)


def initiate_connection(pane):
    pane.send_keys(connection_string)

    start_time = time.time()

    while True:
        if time.time() - start_time > 16:
            print_dots(False)
            print ("\n15 seconds have passed, probably the host is unreachable.\n")
            print ("Check your yaml configuration file and make sure the service is running\n")
            enter_pressed = input('\nPress Enter to exit..')
            if enter_pressed == "":
                sys.exit(1)

        time.sleep(0.5)
        output = pane.capture_pane()
        connected = check_connection(output, connection_string)
        if not connected:
            continue
        else:
            break


def initiate_panes(panes):
    try:
        print('Connecting to ' + dbms)

        initiate_connection(panes[0])
        panes[0].send_keys(autocommit_on)  # so as to terminate any left over transactions

        for create_table_instructions in cfg_table_initialization: #drops table T, and initializes it according to steps in the yml file
            if dbms == 'sqlserver':
                if create_table_instructions == 'COMMIT;;':
                    continue

                panes[0].send_keys('GO')
                time.sleep(0.3)

            panes[0].send_keys(create_table_instructions)

        if dbms == 'sqlserver':
            time.sleep(1)
            panes[0].send_keys('ALTER DATABASE ' + db + ' SET READ_COMMITTED_SNAPSHOT ON;;')
            panes[0].send_keys('GO')
            panes[0].send_keys('ALTER DATABASE ' + db + ' SET ALLOW_SNAPSHOT_ISOLATION ON;;')
            panes[0].send_keys('GO')

        panes[0].send_keys(autocommit_off)
        # panes[0].send_keys(autocommit)
        panes[0].send_keys(clear) # clear table initialization output, so as to show only the Transaction relevant data in the console
        panes[0].send_keys('')

        iter_panes = iter(panes)
        next(iter_panes) # so as to skip the first pane which is being handled above

        for pane in iter_panes:
            # pane.send_keys(connection_string)
            # time.sleep(2)
            initiate_connection(pane)
            pane.send_keys(autocommit_off)
            pane.send_keys(clear)
            pane.send_keys('')

    except HostError as e:
        print_dots(False)
        input('\nUnknown host:\n' + str(e) + '\n\nPress Enter to exit..')
    except DatabaseError as e:
        print_dots(False)
        input('\nDatabase might not exist:\n' + str(e) + '\n\nPress Enter to exit..')
    except AuthenticationError as e:
        print_dots(False)
        input('\nAuthentication Error trying to connect:\n' + str(e) + '\n\nPress Enter to exit..')


def execute_steps(tmux_panes):
    transaction = 'T1'
    pane = None
    print ('\nExecuting test ' + test_comment)
    for steps in cfg_dbms_steps:
        current_Transaction = steps[-2:] # last two characters (e.g. 'T2' from 'step2_T2')
        if transaction != current_Transaction:
            time.sleep(1) # wait 1 second after switching transaction

        if steps[-2:] == 'T1':
            pane = tmux_panes[0] # use the proper pane, depending on the Transaction
        elif steps[-2:] == 'T2':
            pane = tmux_panes[-1] #the last element of the list
        elif steps[-2:] == 'T3':
            pane = tmux_panes[1]

        for transaction_steps in cfg_dbms_steps[steps]:
            pane.send_keys(transaction_steps)   # execute the transaction's steps
            if dbms == 'sqlserver':
                pane.send_keys('GO')


        transaction = current_Transaction

    tmux_panes[0].select_pane()


def run_tmux():
    try:
        tmux_panes = create_tmux_window_and_panes()

        initiate_panes(tmux_panes)

        execute_steps(tmux_panes)

        print_dots(False)
        time.sleep(0.5)
        server.attach_session(target_session=session_name) # show tmux console
    except TypeError as err:
        print_dots(False)
        input('\nYou probably have a formatting error in the yaml file'
              '\nPlease check the syntax, close this window and re-run the test\n'
              '\nError: ' + str(err))


def print_dots(keep_printing):
    global keep_printing_dots
    keep_printing_dots = keep_printing

    while keep_printing_dots:
        time.sleep(0.5)
        sys.stdout.write('.')
        sys.stdout.flush()

    if not keep_printing:
        keep_printing_dots = False

def hide_user_input(hide):
    no_input_terminal = termios.tcgetattr(fd)
    no_input_terminal[3] = no_input_terminal[3] & ~termios.ECHO  # lflags
    if hide:
        termios.tcsetattr(fd, termios.TCSADRAIN, no_input_terminal)
    else:
        termios.tcsetattr(fd, termios.TCSADRAIN, normal_terminal)

def main():
    hide_user_input(True)

    validate_arguments()

    parse_yaml(YAML_FILE)

    prepare_connection()

    thread_tmux = threading.Thread(target = run_tmux)
    thread_tmux.start()

    time.sleep(0.3)
    print_dots(True)

    thread_tmux.join()

if __name__== "__main__":
    main()
