all:runnable_so.exe \
	test_spirit.exe \
	test_bind.exe

runnable_so.exe:runnable_so.o
	g++ -o $@ $^ -shared -e so_main

%.exe:%.o
	g++ -o $@ $^ 

%.o:%.cc
	g++ -o $@ $^ -fPIC -c -g

clean:
	rm -rf *.exe *.o
