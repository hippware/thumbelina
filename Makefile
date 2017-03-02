APP=thumbelina

SRC_FILES=${APP}.py

.PHONY: compile

all: compile ${APP}.zip

${APP}.zip:
	zip $@ ${SRC_FILES}

compile:
	python -m py_compile ${SRC_FILES}
