test_assign4_1: test_assign4_1.o btree_mgr.o record_mgr.o expr.o rm_serializer.o buffer_mgr_stat.o buffer_mgr.o dberror.o storage_mgr.o
	gcc -o test_assign4_1 test_assign4_1.o  btree_mgr.o record_mgr.o expr.o rm_serializer.o buffer_mgr_stat.o buffer_mgr.o dberror.o storage_mgr.o

test_assign4_1.o: test_assign4_1.c buffer_mgr.h expr.h record_mgr.h tables.h test_helper.h dberror.h btree_mgr.h buffer_mgr.h
	gcc -c test_assign4_1.c -lm

test_assign4_2: test_assign4_2.o btree_mgr.o record_mgr.o expr.o rm_serializer.o buffer_mgr_stat.o buffer_mgr.o dberror.o storage_mgr.o
	gcc -o test_assign4_2 test_assign4_2.o  btree_mgr.o record_mgr.o expr.o rm_serializer.o buffer_mgr_stat.o buffer_mgr.o dberror.o storage_mgr.o

test_assign4_2.o: test_assign4_2.c buffer_mgr.h expr.h record_mgr.h tables.h test_helper.h dberror.h btree_mgr.h buffer_mgr.h
	gcc -c test_assign4_2.c -lm

btree_mgr.o: btree_mgr.c btree_mgr.h expr.h record_mgr.h tables.h test_helper.h dberror.h buffer_mgr.h
	gcc -c btree_mgr.c

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h test_helper.h tables.h 
	gcc -c test_expr.c 

record_mgr.o:storage_mgr.h buffer_mgr.h record_mgr.c record_mgr.h  
	gcc -c  record_mgr.c

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	gcc -c expr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	gcc -c rm_serializer.c

buffer_mgr_stat.o: buffer_mgr_stat.h buffer_mgr_stat.c buffer_mgr.h
	gcc -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.h buffer_mgr.c storage_mgr.h dt.h
	gcc -c buffer_mgr.c

dberror.o: dberror.c dberror.h
	gcc -c dberror.c

storage_mgr.o: storage_mgr.h storage_mgr.c
	gcc -c storage_mgr.c
clean: 
	rm *.o

run_test1:
	./test_assign4_1

run_test2:
	./test_assign4_2