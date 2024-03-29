hp:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

hp2:
	@echo " Compile ht_main2 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main2.c ./src/hash_file.c -lbf -o ./build/runner -O2

hp3:
	@echo " Compile ht_main3 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main3.c ./src/hash_file.c -lbf -o ./build/runner -O2

clean:
	@rm data*
	# @rm build/runner*
