"""
Copyright [2017] [ha62791]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


import os
import fcntl
import time
import select
from subprocess import Popen, PIPE


class HBaseShell:

    def __init__(self):
        self.__shell = None
        self.__shell_resp_timeout_sec = 15.0


    @staticmethod
    def __set_non_blocking(fd):
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        flags = flags | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


    @staticmethod
    def __extract_err_msg(output):
        return output[ output.lower().index('error:') : output.index('Here is') ].strip()


    def __run_cmd_wait_output(self, cmd, onNewLinesHandler):
        if not self.isOpen():
            raise RuntimeError("Shell is not open")

        if type(cmd) is not bytes:
            raise ValueError("'cmd' should be of type 'bytes'")

        # append endline
        if not cmd.endswith(os.linesep.encode()):
            cmd += os.linesep.encode()

        p = self.__shell
        p.stdout.read() # clear stdout
        p.stderr.read() # clear stderr

        # input cmd
        p.stdin.write(cmd)
        p.stdin.flush()

        # wait for lines
        endOfOutput = False
        while not endOfOutput:
            if select.select([p.stdout,],[],[],self.__shell_resp_timeout_sec)[0]:
                endOfOutput = onNewLinesHandler(p.stdout.readlines())
            else:
                for line in p.stderr:
                    print(line.decode())

                raise TimeoutError("No response from shell for " + str(int(self.__shell_resp_timeout_sec)) + " seconds." + os.linesep + "Please check HBase health status and reopen HBaseShell.")

        # print stderr
        for line in p.stderr:
            print(line.decode())


    def __run_cmd_default_new_lines_handler(self, cmd, endOfOutput):

        output = ""
        hasError = False

        def onNewLinesHandler(lines):
            nonlocal output
            nonlocal hasError

            for line in lines:
                t_line = line.decode()
                output += t_line

                if t_line.lower().startswith('error:'):
                    hasError = True

            if hasError:
                return output.endswith(os.linesep*3)

            return output.endswith(endOfOutput)


        self.__run_cmd_wait_output(cmd, onNewLinesHandler)

        return (output, hasError)


    def setShellRespTimeout(self, sec):
        if type(sec) is not int or sec < 10:
            raise ValueError("'sec' should be an integer of at least 10")
        else:
            self.__shell_resp_timeout_sec = sec*1.0


    def isOpen(self):
        return self.__shell is not None


    def open(self, pathToHBaseShell):
        if self.isOpen():
            self.close()

        p = Popen([pathToHBaseShell,"shell"], stdin=PIPE, stdout=PIPE, stderr=PIPE, bufsize=1)
        HBaseShell.__set_non_blocking(p.stdout)
        HBaseShell.__set_non_blocking(p.stderr)
        self.__shell = p

        (output, hasError) = self.__run_cmd_default_new_lines_handler(b"", os.linesep*3)
        print(output.strip())


    def close(self):
        if self.isOpen():
            self.__shell.stdin.write(b'exit' + os.linesep.encode())
            self.__shell.stdin.flush()
            time.sleep(1)
            self.__shell.stdout.close()
            self.__shell.stderr.close()
            self.__shell.kill()
            self.__shell = None


    def version(self):
        (output, hasError) = self.__run_cmd_default_new_lines_handler(b"version", os.linesep*2)
        return output.split(os.linesep)[1]


    def whoami(self):
        (output, hasError) = self.__run_cmd_default_new_lines_handler(b"whoami", os.linesep*2)

        lines = output.strip().split(os.linesep)
        t_infos = lines[1].split(' (')
        t_groups = lines[2].strip().replace('groups:', '').split(',')

        return {
            'user': t_infos[0].strip(),
            'auth': t_infos[1].strip()[5:-1].strip(),
            'groups': [ grp.strip() for grp in t_groups ]
        }


    def listTables(self, filterRegex=None):

        cmd = b"list"

        if filterRegex is not None:
            if type(filterRegex) is str:
                cmd = b"list '" + filterRegex.replace("'","\\'").encode() + b"'"
            else:
                raise ValueError("'filterRegex' should be of type 'str'")

        (output, hasError) = self.__run_cmd_default_new_lines_handler(cmd, "]"+os.linesep)

        if hasError:
            raise RuntimeError(HBaseShell.__extract_err_msg(output))

        return output[ output.index('TABLE')+5 : output.index(' row(s) in ') ].strip().split(os.linesep)[:-1]


# Test ERROR
# Test shutdonw master/regionserver
    def printAll(self):
        print('stdout:')
        for line in self.__shell.stdout:
            print(line)

        print('stderr:')
        for line in self.__shell.stderr:
            print(line)