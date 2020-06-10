#!/usr/bin/env python
# coding: utf-8
import sys


def main(jobid):
	if   (jobid == "57336850"): file = open("tests/output/single.out")
	elif (jobid == "57278179"): file = open("tests/output/single_failed.out")
	elif (jobid == "57339555"): file = open("tests/output/single_pending.out")
	elif (jobid == "57340046"): file = open("tests/output/single_running.out")
	elif (jobid == "57340029"): file = open("tests/output/array_running.out")
	elif (jobid == "57340052"): file = open("tests/output/array_completed.out")
	elif (jobid == "57169427"): file = open("tests/output/array_mixed.out")
	elif (jobid == "fail")    : file = open("tests/output/sacct_fail.out")
	else: 						file = open("tests/output/nonexistent.out")

	sys.stdout.write(file.read())

if __name__ == '__main__':
	jobid = sys.argv[1]
	main(jobid)
	