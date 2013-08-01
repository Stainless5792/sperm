#!/usr/bin/env python
#coding:utf-8
#Copyright (C) dirlt

import message_pb2
import time
import httplib
import urllib2

def raiseHTTPRequest(url,data=None,timeout=3):
    # if we do post, we have to provide data.
    f=urllib2.urlopen(url,data,timeout)
    return f.read()

def queryColumnSameConnection():
    print '----------queryColumn----------'
    request = message_pb2.ReadRequest()

    request.table_name='t1'
    request.row_key='r1'
    request.column_family='cf'
    request.qualifiers.append('c2')
    request.qualifiers.append('c1')

    data = request.SerializeToString()

    conn = httplib.HTTPConnection('localhost',8000,timeout=20)
    for i in range(0,3):
        conn.request('GET','/read',data)
        data2 = conn.getresponse().read()
        response = message_pb2.ReadResponse()
        response.ParseFromString(data2)
        print response

def queryColumn():
    print '----------queryColumn----------'
    request = message_pb2.ReadRequest()

    request.table_name='t1'
    request.row_key='r1'
    request.column_family='cf'
    request.qualifiers.append('c2')
    request.qualifiers.append('c1')
    request.timeout = 2

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://localhost:8000/read',data,timeout=20)
    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    print response


def queryEmptyColumn():
    print '----------queryColumn----------'
    request = message_pb2.ReadRequest()

    request.table_name='t1'
    request.row_key='r1'
    request.column_family='cf'
    request.qualifiers.append('not-exits')

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://localhost:8000/read',data,timeout=20)
    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    print response

def queryColumnNone():
    print '----------queryColumnNone----------'
    request = message_pb2.ReadRequest()

    request.table_name='t2'
    request.row_key='r1'
    request.column_family='cf'
    request.qualifiers.append('not-exits')

    data = request.SerializeToString()
    try:
        data2 = raiseHTTPRequest('http://localhost:8000/read',data,timeout=20)
    except Exception,e:
        pass

def queryColumnFamily():
    print '----------queryColumnFamily----------'
    request = message_pb2.ReadRequest()

    request.table_name='t1'
    request.row_key='r1'
    request.column_family='cf'

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://localhost:8000/read',data,timeout=20)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    print response

def multiQuery():
    print '----------multiQuery----------'
    mRequest = message_pb2.MultiReadRequest()

    request = message_pb2.ReadRequest()
    request.table_name='t1'
    request.row_key='r1'
    request.column_family='cf'
    request.qualifiers.append('c2')
    request.qualifiers.append('c1')
    mRequest.requests.extend([request])

    request = message_pb2.ReadRequest()
    request.table_name='t1'
    request.row_key='r1'
    request.column_family='cf'
    mRequest.requests.extend([request])

    mRequest.timeout = 1

    data = mRequest.SerializeToString()
    data2 = raiseHTTPRequest('http://localhost:8000/multi-read',data,timeout=20)
    mResponse = message_pb2.MultiReadResponse()
    mResponse.ParseFromString(data2)
    print mResponse

def multiQueryNone():
    print '----------multiQueryNone----------'
    mRequest = message_pb2.MultiReadRequest()

    request = message_pb2.ReadRequest()
    request.table_name='t2'
    request.row_key='r1'
    request.column_family='cf'
    request.qualifiers.append('c2')
    request.qualifiers.append('c1')
    mRequest.requests.extend([request])

    request = message_pb2.ReadRequest()
    request.table_name='t2'
    request.row_key='r1'
    request.column_family='cf'
    mRequest.requests.extend([request])

    data = mRequest.SerializeToString()
    try:
        data2 = raiseHTTPRequest('http://localhost:8000/multi-read',data,timeout=20)
    except Exception,e:
        pass

    
def write():
    print '----------write----------'
    request = message_pb2.WriteRequest()

    request.table_name = 't1'
    request.row_key = 'r0'
    request.column_family = 'cf'
    kv = request.kvs.add()
    kv.qualifier = 'key'
    kv.content = 'value'

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://localhost:8000/write',data,timeout=20)

    response = message_pb2.WriteResponse()
    response.ParseFromString(data2)
    print response
    
def multiWrite():
    print '----------multiWrite----------'
    mRequest = message_pb2.MultiWriteRequest()

    request = message_pb2.WriteRequest()
    request.table_name = 't1'
    request.row_key = 'r0'
    request.column_family = 'cf'
    kv = request.kvs.add()
    kv.qualifier = 'key'
    kv.content = 'value'
    mRequest.requests.extend([request])

    request = message_pb2.WriteRequest()
    request.table_name = 't1'
    request.row_key = 'r1'
    request.column_family = 'cf'
    kv = request.kvs.add()
    kv.qualifier = 'key'
    kv.content = 'value'
    mRequest.requests.extend([request])

    data = mRequest.SerializeToString()
    data2 = raiseHTTPRequest('http://localhost:8000/multi-write',data,timeout=20)

    mResponse = message_pb2.MultiWriteResponse()
    mResponse.ParseFromString(data2)
    print mResponse

def multiWriteNone():
    print '----------multiWriteNone----------'
    mRequest = message_pb2.MultiWriteRequest()

    request = message_pb2.WriteRequest()
    request.table_name = 't2'
    request.row_key = 'r0'
    request.column_family = 'cf'
    kv = request.kvs.add()
    kv.qualifier = 'key'
    kv.content = 'value'
    mRequest.requests.extend([request])

    request = message_pb2.WriteRequest()
    request.table_name = 't2'
    request.row_key = 'r1'
    request.column_family = 'cf'
    kv = request.kvs.add()
    kv.qualifier = 'key'
    kv.content = 'value'
    mRequest.requests.extend([request])

    data = mRequest.SerializeToString()
    try:
        data2 = raiseHTTPRequest('http://localhost:8000/multi-write',data,timeout=20)
    except Exception,e:
        pass

if __name__=='__main__':
    # queryColumn()
    # queryColumnSameConnection()
    # queryColumn()
    # queryEmptyColumn()
    # queryColumnNone()
    # queryColumnFamily()
    # write()
    multiQuery()
    # multiQueryNone()
    # multiWrite()
    # multiWriteNone()
