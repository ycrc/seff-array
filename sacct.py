#!/usr/bin/env python
# coding: utf-8
import sys


def main(jobid):
	if   (jobid == "57336850"): file = open("tests/data/single.data")
	elif (jobid == "57278179"): file = open("tests/data/single_failed.data")
	elif (jobid == "57339555"): file = open("tests/data/single_pending.data")
	elif (jobid == "57340046"): file = open("tests/data/single_running.data")
	elif (jobid == "57340029"): file = open("tests/data/array_running.data")
	elif (jobid == "57340052"): file = open("tests/data/array_completed.data")
	elif (jobid == "57169427"): file = open("tests/data/array_mixed.data")
	else: 						file = open("tests/data/nonexistent.data")

	sys.stdout.write(file.read())

if __name__ == '__main__':
	jobid = sys.argv[1]
	main(jobid)
	