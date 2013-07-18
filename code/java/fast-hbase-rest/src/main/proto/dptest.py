#!/usr/bin/env python
#coding:utf-8
#Copyright (C) dirlt

import message_pb2
import time

import urllib2
def raiseHTTPRequest(url,data=None,timeout=3):
    # if we do post, we have to provide data.
    f=urllib2.urlopen(url,data,timeout)
    return f.read()

def queryColumn():
    print '----------queryColumn----------'
    request = message_pb2.ReadRequest()

    request.table_name='appbenchmark'
    request.row_key='2012-04-08_YULE'
    request.column_family='stat'
    request.qualifiers.append('14_day_active_count_avg')

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://dp0:12345/read',data,timeout=20)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    #print response

def queryColumn2():
    print '----------queryColumn2----------'
    request = message_pb2.ReadRequest()

    request.table_name='appuserstat'
    request.row_key='2013-03-08_4db949a2112cf75caa00002a'
    request.column_family='stat'
    request.qualifiers.append('provinces_7_lanCnt')

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://dp0:12345/read',data,timeout=20)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    print response

def queryColumnNone():
    print '----------queryColumnNone----------'
    request = message_pb2.ReadRequest()

    request.table_name='appretentionstat'
    request.row_key='2013-03-12_4db949a2112cf75caa00002a'
    request.column_family='stat'
    request.qualifiers.append('provinces_7_lanCnt')

    data = request.SerializeToString()
    try:
        data2 = raiseHTTPRequest('http://dp0:12345/read',data,timeout=20)
    except Exception,e:
        pass

def queryColumnFamily():
    print '----------queryColumnFamily----------'
    request = message_pb2.ReadRequest()

    request.table_name='appbenchmark'
    request.row_key='2012-04-08_YULE'
    request.column_family='stat'

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://dp0:12345/read',data,timeout=20)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)
    #print response

def multiQuery():
    print '----------multiQuery----------'
    mRequest = message_pb2.MultiReadRequest()

    request = message_pb2.ReadRequest()
    request.table_name='appbenchmark'
    request.row_key='2012-04-08_YULE'
    request.column_family='stat'
    request.qualifiers.append('14_day_active_count_avg')
    mRequest.requests.extend([request])

    request = message_pb2.ReadRequest()
    request.table_name='appbenchmark'
    request.row_key='2012-04-08_YULE'
    request.column_family='stat'
    mRequest.requests.extend([request])

    data = mRequest.SerializeToString()
    data2 = raiseHTTPRequest('http://dp0:12345/multi-read',data,timeout=20)
    
    mResponse = message_pb2.MultiReadResponse()
    mResponse.ParseFromString(data2)
    #print mResponse


def queryColumnLarge():
    print '----------queryColumnLarge----------'
    request = message_pb2.ReadRequest()

    request.table_name='appuserstat'
    request.row_key='2013-03-08_4d707f5e112cf75410007470'
    request.column_family='stat'
    request.qualifiers.append('models_1_lanCnt')

    data = request.SerializeToString()
    s = time.time()
    data2 = raiseHTTPRequest('http://dp0:12345/read',data,timeout=20)
    print "data size = %d bytes"%(len(data2))
    e = time.time()
    print 'time spent %.2lfs'%((e-s))

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)

    x = response.kvs[0].content
    xs = x.split("\f")
    print "models size = %d"%(len(xs))

def queryModels():
    print '----------Models----------'
    request = message_pb2.ReadRequest()

    request.table_name = 'appuserstat'
    request.row_key = '2013-07-02_4d707f5e112cf75410007470'
    request.column_family = 'stat'
    request.qualifiers.append('models_1_installCnt')

    data = request.SerializeToString()
    data2 = raiseHTTPRequest('http://dp0:12345/read',data,timeout=20)

    response = message_pb2.ReadResponse()
    response.ParseFromString(data2)

    xs = response.kvs[0].content.split("\0")
    for x in xs[:100]:
        ss = x.split("\f")
        print ss
    #print response

if __name__=='__main__':
    # queryColumn()
    # queryColumnFamily()
    # multiQuery()
    # queryColumnNone()
    # queryColumnLarge()
    queryModels()
    
