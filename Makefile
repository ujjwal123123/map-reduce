.DEFAULT: build
.PHONY: clean build test
SDIFF_FLAGS = -s
SLOTS_COUNT = 10

build: combiner

combiner: combiner.c
	gcc -g3 -O0 -Wall -Wextra -pthread -o combiner combiner.c

test0: build
	./combiner $(SLOTS_COUNT) 3 < testcases/input0.txt | sort | sdiff $(SDIFF_FLAGS) testcases/output0.sorted -

test1: build
	./combiner $(SLOTS_COUNT) 5 < testcases/input1.txt | sort | sdiff $(SDIFF_FLAGS) testcases/output1.sorted -

test2: build
	./combiner $(SLOTS_COUNT) 10 < testcases/input2.txt | sort | sdiff $(SDIFF_FLAGS) testcases/output2.sorted -

test3: build
	./combiner $(SLOTS_COUNT) 10 < testcases/input3.txt | sort | sdiff $(SDIFF_FLAGS) testcases/output3.sorted -

test4: build
	./combiner $(SLOTS_COUNT) 20 < testcases/input4.txt | sort | sdiff $(SDIFF_FLAGS) testcases/output4.sorted -

test: test0 test1 test2 test3 test4

clean:
	rm -f combiner
