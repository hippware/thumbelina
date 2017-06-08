APP=thumbelina
PGLIB_SRC=awslambda-psycopg2
PGLIB=psycopg2
ZIP=${APP}.zip

SRC_FILES=${APP}.py
DST_FILES=${APP}.pyc

.PHONY: compile clean

all: compile ${APP}.zip

clean:
	-rm ${ZIP} *.pyc ${PGLIB}

${ZIP}: ${DST_FILES}
	-ln -s ${PGLIB_SRC}/${PGLIB} ${PGLIB}
	zip -r $@ ${SRC_FILES} ${PGLIB}

%.pyc: %.py
	python -m py_compile $<

compile: ${DST_FILES}
