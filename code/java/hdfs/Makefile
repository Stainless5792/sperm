#coding:utf-8
#Copyright (C) dirlt

# mostly this works.
copy-unpack-dependency:
	mvn dependency:unpack-dependencies

# if the above command failed. please use this one.
copy-unpack-dependency2:
	mvn dependency:copy-dependencies ; cd ./target/dependency ; for x in *.jar ; do echo "jar xf $$x ..." ; jar xf $$x ; done

