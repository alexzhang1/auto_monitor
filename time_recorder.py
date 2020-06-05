#!/usr/bin/python
# coding=gbk
'''
Created on 2017-11-30

@author: zhang.jx
'''

import time

def timeit(func):
    def wrapper(*args,**kwargs):
        start = time.clock()
        ret = func(*args,**kwargs)
        end =time.clock()
        print('used:', end - start)
        return ret
    return wrapper


@timeit
def foo(a,b,c,d):
    sum = a+b+c+d
    print('in foo()')
    time.sleep(1)
    return 'ret'

if __name__ == '__main__':
    print(foo(1,2,3,4))