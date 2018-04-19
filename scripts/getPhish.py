# -*- coding=utf-8 -*-
from sys import argv
import string
import re

tag_re = re.compile(r'<[^>]+>')

def getWords(w):
	f = open(w, "r")
	words = []
	for i in f:
		words.append(i.replace("\n",""))

	return words

def getPhish():

	f_in_msg = open(argv[1], "r")
	f_in_header = open(argv[2], "r")
	count_words = {}

	#words = getWords(argv[2])

	approach = getWords(argv[3])
	money = getWords(argv[4])
	reply = getWords(argv[5])
	urgency = getWords(argv[6])
	form = getWords(argv[7])
	security = getWords(argv[8])
	dynamic = getWords(argv[9])

	for line in f_in_msg:
		h_line = f_in_header.readline()
		if len(h_line.split("\t")) >= 9 :
			rcpt_list = h_line.split("\t")[8]
			rcpts = rcpt_list.split(";")
		else:
			rcpts = ["n/a,-1"]

		score = 0
		scr_approach = 0; scr_money = 0; scr_reply = 0;
		scr_urgency = 0; scr_form = 0; scr_security = 0;
		scr_dynamic = 0; scr_direct = 0;
		tokens = line.split(" ")
		for t in tokens:
			if t in approach:
				scr_approach = 1
			elif t in money or t[0] == "$":
				scr_money = 1
			elif t in reply:
				scr_reply = 1
			elif t in urgency:
				scr_urgency = 1
			elif t in form:
				scr_form = 1
			elif t in security:
				scr_security = 1
			elif t in dynamic:
				scr_dynamic = 1
			if len(rcpts) == 1:
				if len(rcpts[0].split(",")) > 1:
					if rcpts[0].split(",")[1] == 1:
						scr_direct = 1

		#if score > 4:
		score = scr_approach + scr_money + scr_reply + scr_urgency + scr_form + scr_security + scr_dynamic + scr_direct
		print score

getPhish()
