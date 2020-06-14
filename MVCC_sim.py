#!/usr/bin/python
"""
User chooses preferred DBMS and test scenario,
a tmux window opens and the transaction steps run in the specified order.
Transaction steps are to be found in mvcc_tests.yml file.
Up to three concurrent transactions are supported.
Supported DBMSs are: MySQL | Oracle | SQL Server | PostgreSQL

Can run in the following ways:

1.  $ python MVCC_sim.py
    It will open a menu to choose DBMS and then
    a menu to choose test scenario for the selected DBMS.
    After choosing a test scenario, a tmux window will open
    which will run the test steps. First window will persist.
2.  $ python MVCC_sim.py <dbms>
    It will open a menu to choose from the DBMS's test scenarios.
    After choosing a test scenario, a tmux window will open
    which will run the test steps. First window will persist.
3.  $ python MVCC_sim.py <dbms> <test_num>
    A tmux window will open which will run the test steps.
------------------------------------------------------------------------------
Author: Konstantinos Diamantidis - March 2020
------------------------------------------------------------------------------
"""


import time
import os
import sys
import subprocess
import curses



def install_modules():
    """
    Installs the necessary modules.

    The following get installed if they don't exist:
    1. pip (python 2 check)
    (used for installing the following modules)
    2. libtmux (v0.8.2)
    (used for opening the tmux server/session)
    3. yamlordereddictloader (v0.4.0)
    (used for parsing the yaml file in the correct order)
    4. pyyaml (v5.3.1)
    (used for yaml file handling)

    :return: None
    """
    import importlib
    import site

    try:
        # checks if pip is installed
        proc = subprocess.Popen(['python', '-m', 'pip', '--version'],
                                stdout=subprocess.PIPE)
        proc.wait()

        # poll() returns subprocess's exit code (1 means that it failed)
        if proc.poll():
            print('Seems like pip is not installed.\n Trying to Install PIP...\n')
            os.system('curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py')
            os.system('python get-pip.py')
            print('\n Removing get-pip.py...\n')
            os.system('rm get-pip.py')
            print('\nScreen will now clear')
            time.sleep(3)
            os.system('clear')
    except Exception as err:
        print(str(err))
        time.sleep(1)

    try:
        import libtmux
    except ImportError as err:
        if 'libtmux' in str(err):
            print ("Trying to Install required module: libtmux\n")
            subprocess.check_call([
                    sys.executable, "-m",
                    "pip", "install", 'libtmux==0.8.2'
            ])
            print('\nScreen will now clear')

            # so that the installed module can be imported
            # without having to rerun the script
            reload(site)
            globals()['libtmux'] = importlib.import_module('libtmux')
            time.sleep(3)
            os.system('clear')
    try:
        import yamlordereddictloader
    except ImportError as err:
        if 'yamlordereddictloader' in str(err):
            print ("Trying to Install required module: yamlordereddictloader\n")
            subprocess.check_call([
                    sys.executable, "-m",
                    "pip", "install", 'yamlordereddictloader==0.4.0'
            ])
            print('\nScreen will now clear')
            reload(site)
            globals()['yamlordereddictloader'] = importlib.import_module('yamlordereddictloader')
            time.sleep(3)
            os.system('clear')
    try:
        import yaml
    except ImportError as err:
        if 'yaml' in str(err):
            print ("Trying to Install required module: yaml\n")
            subprocess.check_call([
                    sys.executable, "-m",
                    "pip", "install", 'pyyaml=5.3.1'
            ])
            print('\nScreen will now clear')
            reload(site)
            globals()['yaml'] = importlib.import_module('yaml')
            time.sleep(3)
            os.system('clear')


def install_tmux():
    """
    Installs tmux package through apt.

    Tmux is a terminal multiplexer which enables a number of terminals
    to be created, accessed, and controlled from a single screen.

    :return: None
    """
    try:
        import apt
        cache = apt.Cache()
        if not cache['tmux'].is_installed:
            print ("Seems like tmux is not installed.\n"
                   "Trying to Install tmux...\n")
            os.system('sudo apt-get install tmux=2.8-3')
            print('\nScreen will now clear')
            time.sleep(3)
            os.system('clear')
    except ImportError as err:
        if 'No module named \'apt\'' in str(err):
            print ('\nSeems like python3-apt is not installed')
            print ('\nWill now try to install python3-apt ...\n')
            time.sleep(3)
            os.system('sudo apt-get install python3-apt')
            time.sleep(3)
            install_tmux()
        else:
            print(str(err))
            time.sleep(1)


install_modules()
install_tmux()

from mvcc_runner import find_comment
from mvcc_runner import find_comments
from mvcc_runner import is_dbms_running
from mvcc_runner import SUPPORTED_DBMS

YAML_FILE = "./mvcc_tests.yml"

KEYS_ENTER = (curses.KEY_ENTER, ord('\n'), ord('\r'))
KEYS_UP = (curses.KEY_UP, ord('k'))
KEYS_DOWN = (curses.KEY_DOWN, ord('j'))
DBMS = None
TESTS_RUN_LINE = []  # initialized as empty so as not to be displayed if no tests have been run
WHICH_TESTS_RUN = []
EXIT_OPTION = ['~ EXIT ~']


def restart_dbms(dbms):
    """
    Opens a new terminal window and restarts the selected dbms service.
    When service gets restarted, the window closes.

    :param dbms: 'oracle' | 'mysql' | 'postgres' | 'sqlserver'
    :return: None
    """
    dbms_service = ''
    if "debian" in str(os.uname()):
        if dbms == 'oracle':
            dbms_service = 'oracle-xe'
        elif dbms == 'postgres':
            dbms_service = 'postgresql'
        elif dbms == 'sqlserver':
            dbms_service = 'mssql-server'
        elif dbms == 'mysql':
            dbms_service = 'mysql'

    service_restart_command = 'sudo systemctl restart ' + dbms_service

    subprocess.call([
        'x-terminal-emulator',
        '-title', 'Restarting ' + dbms + ' Please wait..',
        '-geometry', '60x10',
        '-e', service_restart_command
    ])


def run_scenario(dbms, test_num, test_comment, yamlfile):
    """
    Opens a new temrinal window and executes ./mvcc_runner.py
    which runs the selected test scenario in a tmux session.

    :param dbms: 'oracle' | 'mysql' | 'postgres' | 'sqlserver'
    :param test_num: the number of the test scenario to be run (e.g. 'test4')
    :param test_comment: the comment that accompanies the selected test in the yaml file
                        e.g. (test 4 # Anomaly|Lost Update - Isolation|Serializable)
    :param yamlfile: the location of the yaml file. Hardcoded at the moment to "./mvcc_tests.yml"
    :return: None
    """
    subprocess.call([
        'x-terminal-emulator',
        '-title', dbms.upper() + ' - ' + test_num.upper() + ' - ' + test_comment,
        '-geometry', '150x52',
        '-e', 'python ./mvcc_runner.py ' + dbms + ' ' + test_num + ' ' + yamlfile
    ])


def test_selection_handler(this):
    """
    Callback used for handling the test scenario selected
    from the curses menu.
    Upon selecting a test scenario, by pressing Enter,
    test gets marked as ran (will be printed on the bottom of the screen)
    and a new terminal window opens and runs the test in a tmux session.

    :param this: will accept Picker's self
    :return: None
    """
    try:
        global WHICH_TESTS_RUN

        selected_test = this.index + 1

        if selected_test == len(this.options):
            # last option, which must be 'EXIT PROGRAM'
            sys.exit(1)

        if selected_test == len(this.options) - 1:
            # previous to last option, which must be 'Restart dbms service'
            restart_dbms(DBMS)
            return

        if selected_test not in WHICH_TESTS_RUN:
            WHICH_TESTS_RUN = WHICH_TESTS_RUN + [selected_test]
            WHICH_TESTS_RUN.sort()

        test_num = 'test' + str(selected_test)

        test_comment = find_comment(YAML_FILE, DBMS, test_num)
        run_scenario(DBMS, test_num, test_comment, YAML_FILE)
    except Exception as err:
        input(str(err))
        sys.exit(0)


class Picker(object):
    """The :class:`Picker <Picker>` object

    :param options: a list of options to choose from
    :param title: (optional) a title above options list
    :param indicator: (optional) custom the selection indicator
    :param default_index: (optional) set this if the default selected option is not the first one
    """

    def __init__(self, options, title=None, indicator='*', default_index=0):

        if not options:
            raise ValueError('options should not be an empty list')

        self.options = options
        self.title = title
        self.indicator = indicator
        self.scroll_top = getattr(self, 'scroll_top', 0)
        self.screen = None
        self.callback = None

        if default_index >= len(options):
            raise ValueError('default_index should be less than the length of options')

        self.index = default_index

    def move_up(self):
        self.index -= 1
        if self.index < 0:
            self.index = len(self.options) - 1

    def move_down(self):
        self.index += 1
        if self.index >= len(self.options):
            self.index = 0

    def get_selected(self):
        """return the current selected option as a tuple: (option, index)"""
        return self.options[self.index], self.index

    def get_title_lines(self):
        if self.title:
            return self.title.split('\n') + ['']
        return []

    def get_option_lines(self):
        lines = []
        for index, option in enumerate(self.options):
            if index == self.index:
                prefix = self.indicator
            else:
                prefix = len(self.indicator) * ' '

            # will paint the color blue in the selected line
            if index + 1 in WHICH_TESTS_RUN:
                color_pair = curses.color_pair(1)
                line = ('{0} {1}'.format(prefix, option), color_pair)
            else:
                line = '{0} {1}'.format(prefix, option)
            lines.append(line)

        return lines

    def get_lines(self):
        title_lines = self.get_title_lines()
        option_lines = self.get_option_lines()

        if WHICH_TESTS_RUN:
            global TESTS_RUN_LINE
            TESTS_RUN_LINE = ['\nTests already run: ']

        lines = title_lines + option_lines + TESTS_RUN_LINE
        current_line = self.index + len(title_lines) + 1

        return lines, current_line

    def draw(self):
        """draw the curses ui on the screen, handle scroll if needed"""
        try:
            self.screen.clear()

            x, y = 1, 1  # start point
            max_y, max_x = self.screen.getmaxyx()
            max_rows = max_y - y  # the max rows we can draw

            lines, current_line = self.get_lines()

            # calculate how many lines we should scroll, relative to the top
            scroll_top = getattr(self, 'scroll_top', 0)
            if current_line <= scroll_top:
                scroll_top = 0
            elif current_line - scroll_top > max_rows:
                scroll_top = current_line - max_rows
            self.scroll_top = scroll_top

            lines_to_draw = lines[scroll_top:scroll_top+max_rows]

            for line in lines_to_draw:
                # if type(line) is tuple:
                if isinstance(line, tuple):
                    self.screen.addnstr(y, x, line[0], max_x-2, line[1])
                else:
                    self.screen.addnstr(y, x, line, max_x-2)
                y += 1

            if WHICH_TESTS_RUN:
                self.screen.addstr(str(WHICH_TESTS_RUN))

            self.screen.refresh()
        except:
            pass

    def run_loop(self, callback=None):
        while True:
            self.draw()
            c = self.screen.getch()
            if c in KEYS_UP:
                self.move_up()
            elif c in KEYS_DOWN:
                self.move_down()
            elif c in KEYS_ENTER:
                # it will run the provided callback
                # callback was provided when picker.start was called
                # e.g. picker.start(test_selection_handler)
                if callback:
                    callback(self)
                else:
                    # return the selected option text and index number
                    return self.get_selected()

    @staticmethod
    def config_curses():
        try:
            # use the default colors of the terminal
            curses.use_default_colors()
            # hide the cursor
            curses.curs_set(0)
            curses.init_pair(1, curses.COLOR_BLUE, curses.COLOR_WHITE)
        except:
            # Curses failed to initialize color support, eg. when TERM=vt100
            curses.initscr()

    def _start(self, screen):
        self.screen = screen
        self.config_curses()
        return self.run_loop(self.callback)

    def start(self, callback=None):
        if callback:
            self.callback = callback

        return curses.wrapper(self._start)


def main():
    try:
        global DBMS

        if len(sys.argv) < 2:
            picker = Picker(SUPPORTED_DBMS, 'Choose a dbms', '==>')
            option = picker.start()[0]
            DBMS = option

        if len(sys.argv) >= 2:
            if str(sys.argv[1]) in SUPPORTED_DBMS:
                DBMS = sys.argv[1]
            else:
                print('Invalid DBMS name!\nSupported DBMSs are: '
                      + str(SUPPORTED_DBMS))
                sys.exit(0)

        if not is_dbms_running(DBMS):
            print(DBMS + ' is not running')
            sys.exit(0)

        if len(sys.argv) == 3:
            test_num = sys.argv[2]
            test_comment = find_comment(YAML_FILE, DBMS, test_num)
            run_scenario(DBMS, test_num, test_comment, YAML_FILE)
        else:
            comments = find_comments(YAML_FILE, DBMS)
            title = 'Choose a test to run in ' + DBMS + ' dbms:'
            restart_dbms_option = [
                '~ Restart ' + DBMS +
                ' ~ (if an error occurs, restart the service and re-run the test)'
            ]
            picker = Picker(comments + restart_dbms_option + EXIT_OPTION, title, '==>')
            picker.start(test_selection_handler)

    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as err:
        input(str(err))
        sys.exit(0)


if __name__ == "__main__":
    main()
