#!/usr/bin/python
#-*-coding:utf-8-*-
import time
import os
import sys
import subprocess
import curses
import importlib
import site
from mvcc_runner import find_comment
from mvcc_runner import find_comments
from mvcc_runner import is_dbms_running
from mvcc_runner import supported_dbms


def installLibraries():
    try:
        p = subprocess.Popen(['python', '-m', 'pip', '--version'],
                             stdout=subprocess.PIPE)
        p.wait()
        # poll() returns subprocess's exit code (1 means that it failed)
        if p.poll():
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
            subprocess.check_call([sys.executable, "-m",
                                   "pip", "install", 'libtmux==0.8.2'])
            print('\nScreen will now clear')
            reload(site)
            globals()['libtmux'] = importlib.import_module('libtmux')
            time.sleep(3)
            os.system('clear')
    try:
        import yamlordereddictloader
    except ImportError as err:
        if 'yamlordereddictloader' in str(err):
            print ("Trying to Install required module: yamlordereddictloader\n")
            subprocess.check_call([sys.executable, "-m",
                                   "pip", "install", 'yamlordereddictloader==0.4.0'])
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
            subprocess.check_call([sys.executable, "-m",
                                   "pip", "install", 'pyyaml=5.3.1'])
            print('\nScreen will now clear')
            reload(site)
            globals()['yaml'] = importlib.import_module('yaml')
            time.sleep(3)
            os.system('clear')


def installTmux():
    try:
        import apt
        cache = apt.Cache()
        if not cache['tmux'].is_installed:
            print ("Seems like tmux is not installed.\nTrying to Install tmux...\n")
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
            installTmux()
        else:
            print(str(err))
            time.sleep(1)

installLibraries()
installTmux()

YAML_FILE = "./mvcc_tests.yml"

KEYS_ENTER = (curses.KEY_ENTER, ord('\n'), ord('\r'))
KEYS_UP = (curses.KEY_UP, ord('k'))
KEYS_DOWN = (curses.KEY_DOWN, ord('j'))
TESTS_RUN_LINE = []
EXIT_LINE = ['~ EXIT ~']
WHICH_TESTS_RUN = []
dbms = None


def restart_dbms(dbms):
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

    service_restart_command = 'sudo systemctl restart ' + dbms_service
    subprocess.call(
        ['x-terminal-emulator', '-title', 'Restarting ' + dbms + ' Please wait..', '-geometry',
         '60x10', '-e', service_restart_command])


def run_scenario(dbms, test_num, test_comment, yamlfile):
    subprocess.call(['x-terminal-emulator',
                     '-title', dbms.upper() + ' - ' + test_num.upper() + ' - ' + test_comment,
                     '-geometry', '150x52',
                     '-e', 'python ./mvcc_runner.py ' + dbms + ' ' + test_num + ' ' + yamlfile])


def test_selection_handler(self):
    try:
        global WHICH_TESTS_RUN

        selected_test = self.index + 1

        if selected_test == len(self.options):
            # last option, which must be 'EXIT PROGRAM'
            sys.exit(1)

        if selected_test == len(self.options) - 1:
            # previous to last option, which must be 'Restart dbms service'
            restart_dbms(dbms)
            return

        if selected_test not in WHICH_TESTS_RUN:
            WHICH_TESTS_RUN = WHICH_TESTS_RUN + [selected_test]
            WHICH_TESTS_RUN.sort()

        test_num = 'test' + str(selected_test)

        test_comment = find_comment(YAML_FILE, dbms, test_num)
        run_scenario(dbms, test_num, test_comment, YAML_FILE)
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

        if len(options) == 0:
            raise ValueError('options should not be an empty list')

        self.options = options
        self.title = title
        self.indicator = indicator
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

            if index + 1 in WHICH_TESTS_RUN:
                format = curses.color_pair(1)
                line = ('{0} {1}'.format(prefix, option), format)
            else:
                line = '{0} {1}'.format(prefix, option)
            lines.append(line)

        return lines

    def get_lines(self):
        title_lines = self.get_title_lines()
        option_lines = self.get_option_lines()

        if not len(WHICH_TESTS_RUN) == 0:
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
                if type(line) is tuple:
                    self.screen.addnstr(y, x, line[0], max_x-2, line[1])
                else:
                    self.screen.addnstr(y, x, line, max_x-2)
                y += 1

            if not len(WHICH_TESTS_RUN) == 0:
                self.screen.addstr(str(WHICH_TESTS_RUN))

            self.screen.refresh()
        except:
            pass

    def run_loop(self, callback = None):
        while True:
            self.draw()
            c = self.screen.getch()
            if c in KEYS_UP:
                self.move_up()
            elif c in KEYS_DOWN:
                self.move_down()
            elif c in KEYS_ENTER:
                if callback:
                    callback(self)
                else:
                    return self.get_selected()

    def config_curses(self):
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

    def start(self, callback = None):
        if callback:
            self.callback = callback

        return curses.wrapper(self._start)



def main():
    try:
        global dbms

        if len(sys.argv) < 2:
            picker = Picker(supported_dbms, 'Choose a dbms', '==>')
            option, index = picker.start()
            dbms = option

        if len(sys.argv) == 2:
            if str(sys.argv[1]) in supported_dbms:
                dbms = sys.argv[1]
            else:
                print('Invalid DBMS name!\nSupported DBMSs are: ' + str(supported_dbms))
                sys.exit(0)

        if not is_dbms_running(dbms):
            print(dbms + ' is not running')
            sys.exit(0)

        comments = find_comments(YAML_FILE, dbms)
        title = 'Choose a test to run in ' + dbms + ' dbms:'
        restart_dbms = ['~ Restart ' + dbms +
                        ' ~ (if an error occurs, restart the service and re-run the test)']
        picker = Picker(comments + restart_dbms + EXIT_LINE, title, '==>')
        picker.start(test_selection_handler)

        if len(sys.argv) == 3:
            test_num = sys.argv[2]
            test_comment = find_comment(YAML_FILE, dbms, test_num)
            run_scenario(dbms, test_num, test_comment, YAML_FILE)

    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as err:
        input(str(err))
        sys.exit(0)

if __name__ == "__main__":
    main()
