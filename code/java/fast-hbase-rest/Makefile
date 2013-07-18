#coding:utf-8
#Copyright (C) dirlt

CP=./target/dependencies;./target/classes

copy-unpack-dependency:
	mvn dependency:unpack-dependencies
proto-py:
	protoc --python_out=./ src/main/proto/message.proto

