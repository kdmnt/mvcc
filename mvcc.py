#!/usr/bin/python
#-*-coding:utf-8-*-

import curses
from mvcc_runner import find_comment
from mvcc_runner import find_comments
from mvcc_runner import is_dbms_running
from mvcc_runner import supported_dbms
import subprocess
import sys

YAML_FILE = "./mvcc_tests.yml"

KEYS_ENTER = (curses.KEY_ENTER, ord('\n'), ord('\r'))
KEYS_UP = (curses.KEY_UP, ord('k'))
KEYS_DOWN = (curses.KEY_DOWN, ord('j'))
TESTS_RUN_LINE = ['\nTests already run: ']
EXIT_LINE = ['EXIT PROGRAM']
WHICH_TESTS_RUN = []
dbms = None

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

        lines = title_lines + option_lines + TESTS_RUN_LINE
        current_line = self.index + len(title_lines) + 1
        return lines, current_line

    def draw(self):
        """draw the curses ui on the screen, handle scroll if needed"""
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

        self.screen.addstr(str(WHICH_TESTS_RUN))
        self.screen.refresh()

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
                    return
                # global WHICH_TESTS_RUN
                #
                # selected_test = self.index + 1
                #
                # if selected_test not in WHICH_TESTS_RUN:
                #     WHICH_TESTS_RUN = WHICH_TESTS_RUN + [selected_test]
                #     WHICH_TESTS_RUN.sort()
                #
                # if selected_test == len(self.options): # last option, which must be 'EXIT PROGRAM'
                #     sys.exit(1)
                #
                # test_num = 'test' + str(selected_test)
                #
                # test_comment = find_comment(YAML_FILE, dbms, test_num)
                # run_scenario(dbms, test_num, test_comment, YAML_FILE)
                # subprocess.call(['x-terminal-emulator', '-title',
                #                  dbms.upper() + ' - ' + test_num.upper() + ' - ' + test_comment, '-geometry',
                #                  '150x52', '-e',
                #                  'python mvcc_runner.py ' + dbms + ' ' + test_num + ' ' + YAML_FILE])

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
        return self.run_loop(callback)

    def start(self):
        return curses.wrapper(self._start)

# def pick(*args, **kwargs):
#     picker = Picker(*args, **kwargs)
#     return picker.start()

def run_scenario(dbms, test_num, test_comment, yamlfile):
    subprocess.call(
        ['x-terminal-emulator', '-title', dbms.upper() + ' - ' + test_num.upper() + ' - ' + test_comment, '-geometry',
         '150x52', '-e', 'python ./mvcc_runner.py ' + dbms + ' ' + test_num + ' ' + yamlfile])

def callback(this):
    global WHICH_TESTS_RUN

    selected_test = this.index + 1

    if selected_test not in WHICH_TESTS_RUN:
        WHICH_TESTS_RUN = WHICH_TESTS_RUN + [selected_test]
        WHICH_TESTS_RUN.sort()

    if selected_test == len(this.options):  # last option, which must be 'EXIT PROGRAM'
        sys.exit(1)

    test_num = 'test' + str(selected_test)

    test_comment = find_comment(YAML_FILE, dbms, test_num)
    run_scenario(dbms, test_num, test_comment, YAML_FILE)


def main():
    try:
        global dbms

        if len(sys.argv) < 2:
            input('Argument error \nMake sure you provide <dbms> and optionally <testNum>')
            sys.exit(0)

        if str(sys.argv[1]) in supported_dbms:
            dbms = sys.argv[1]
        else:
            input('Invalid DBMS name!\nSupported DBMSs are: ' + str(supported_dbms))
            sys.exit(0)

        if not is_dbms_running(dbms):
            input(dbms + ' is not running')
            sys.exit(0)

        if len(sys.argv) == 2:
            comments = find_comments(YAML_FILE, dbms)
            title = 'Choose a test to run in ' + dbms + ' dbms:'
            picker = Picker(comments + EXIT_LINE, title)
            picker.indicator = '==> '
            picker.start()
        else:
            test_num = sys.argv[2]
            test_comment = find_comment(YAML_FILE, dbms, test_num)
            # subprocess.call(['x-terminal-emulator', '-title', dbms.upper() + ' - ' + test_num.upper() + ' - ' + test_comment, '-geometry', '150x52', '-e', 'python mvcc_runner.py ' + dbms + ' ' + test_num + ' ' + YAML_FILE])
            run_scenario(dbms, test_num, test_comment, YAML_FILE)
    except KeyboardInterrupt:
        sys.exit(0)

if __name__== "__main__":
    main()
