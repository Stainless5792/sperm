#!/usr/bin/env python
#coding:utf-8
#Copyright (C) dirlt

import message_pb2
import time

import urllib2
import httplib

def raiseHTTPRequest(url,data=None,timeout=3):
    # if we do post, we have to provide data.
    f=urllib2.urlopen(url,data,timeout)
    return f.read()

def queryColumn(times=100):
    print '----------queryColumn----------'
    request = message_pb2.ReadRequest()

    request.table_name='appuserstat'
    request.row_key='2012-12-19_4c14d4471d41c86c6400072b'
    request.column_family='stat'
    request.qualifiers.append('duTime_d_1_3')
    request.qualifiers.append('duTime_d_4_10')
    request.qualifiers.append('duTime_d_11_30')
    request.qualifiers.append('duTime_d_31_60')
    request.qualifiers.append('duTime_d_61_180')
    request.qualifiers.append('duTime_d_181_600')
    request.qualifiers.append('duTime_d_601_1800')
    request.qualifiers.append('duTime_d_1801_')

    data = request.SerializeToString()
    # long-lived connection.
    conn = httplib.HTTPConnection('dp0',12345,timeout=20)
    conn.connect()
    conn.sock.settimeout(20) # 20 seconds.
    s = time.time()
    for i in xrange(0,times):
        conn.request('GET','/read',data)
        try:
            data2=conn.getresponse().read()
        except:
            print 'fast timeout'
    e = time.time()    
    print 'fast time spent %lf / %d, avg %.4lf'%((e-s),times, (e-s)/times)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    #print response

    s = time.time()
    conn = httplib.HTTPConnection('dp30',8080,timeout=20)    
    for i in xrange(0,times):
        conn.request('GET','/appuserstat/2012-12-19_4c14d4471d41c86c6400072b/stat:duTime_d_1_3,stat:duTime_d_4_10,stat:duTime_d_11_30,stat:duTime_d_31_60,stat:duTime_d_61_180,stat:duTime_d_181_600,stat:duTime_d_601_1800,stat:duTime_d_1801_?v=1',
                     headers = {'Keep-Alive':'timeout=10'})
        try:
            data2 = conn.getresponse().read()
        except:
            print 'rest timeout'
        
    e = time.time()
    print 'rest time spent %lf / %d, avg %.4lf'%((e-s),times,(e-s)/times)

def queryColumnFamily(times=5):
    print '----------queryColumnFamily----------'
    request = message_pb2.ReadRequest()

    request.table_name='appuserstat'
    request.row_key='2012-12-19_4c14d4471d41c86c6400072b'
    request.column_family='stat'

    data = request.SerializeToString()
    # long-lived connection.
    conn = httplib.HTTPConnection('dp0',12345,timeout=20)
    conn.connect()
    conn.sock.settimeout(20) # 20 seconds.
    s = time.time()
    for i in xrange(0,times):
        conn.request('GET','/read',data)
        try:
            data2=conn.getresponse().read()
        except:
            print 'fast timeout'
    e = time.time()
    print 'fast time spent %lf / %d, avg %.4lf'%((e-s),times, (e-s)/times)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    #print response

    s = time.time()
    conn = httplib.HTTPConnection('dp30',8080,timeout=20)    
    for i in xrange(0,times):
        try:
            conn.request('GET','/appuserstat/2012-12-19_4c14d4471d41c86c6400072b/stat:',
                         headers = {'Keep-Alive':'timeout=10'})
        except:
            print 'rest timeout'
        data2 = conn.getresponse().read()
    e = time.time()
    print 'rest time spent %lf / %d, avg %.4lf'%((e-s),times,(e-s)/times)

if __name__=='__main__':
    queryColumn()
    queryColumnFamily()
