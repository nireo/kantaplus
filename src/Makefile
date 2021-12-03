objects = main.o db.o lock.o errors.o

edit : $(objects)
	cc -o edit $(objects)

main.o:../src/db.h
db.o: ../src/lock.h ../src/errors.h

.PHONY:clean
clean:
	rm edit $(objects)
