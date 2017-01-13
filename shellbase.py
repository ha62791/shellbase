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
from subprocess import Popen, PIPE


def setNonBlocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    flags = flags | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)


class HBaseShell:

    def __init__(self):
        self.__shell = None


    def isOpen(self):
        return self.__shell is not None


    def open(self, pathToHBaseShell):
        if self.isOpen():
            return

        p = Popen([pathToHBaseShell,"shell"], stdin=PIPE, stdout=PIPE, stderr=PIPE, bufsize=1)
        setNonBlocking(p.stdout)
        setNonBlocking(p.stderr)
        self.__shell = p


    def close(self):
         if self.isOpen():
              self.__shell.stdin.write(b'exit\n')
              self.__shell.stdin.flush()
              time.sleep(1)
              self.__shell.stdout.close()
              self.__shell.stderr.close()
              self.__shell.kill()
              self.__shell = None