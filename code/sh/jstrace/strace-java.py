#!/usr/bin/env python
#coding:utf-8
#Copyright (C) dirlt

import re
import os
import sys

def getAttempt(arg):
    m = re.search(r'attempt_\d{12}_\d{5}\_[mr]_\d{6}_\d+',arg)
    if m:
        return m.group(0)

def main(args):
    java = '/usr/lib/jvm/java-6-sun/jre/bin/.java'
    for arg in args:
        attempt = getAttempt(arg)
        if attempt:
            break
    #attempt = None
    if attempt:
        binary = '/usr/bin/strace'
        nargs = [binary,'-c','-o','/tmp/strace-java.%s'%(attempt),java]
    else:
        binary = java
	nargs = [java]
    nargs.extend(args[1:])
    os.execvpe(binary,nargs,os.environ)

if __name__ == '__main__':
    main(sys.argv)
        
