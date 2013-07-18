#coding:utf-8
#Copyright (C) dirlt

CP=./target/dependencies;./target/classes

copy-unpack-dependency:
	mvn dependency:unpack-dependencies

map_perf_test:map_perf_test.cc
	g++ -o $@ $^ -shared -fPIC -ltcmalloc
clean:
	rm -rf map_perf_test
