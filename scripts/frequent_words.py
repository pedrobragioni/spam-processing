# -*- coding=utf-8 -*-
from sys import argv
import string
import re

tag_re = re.compile(r'<[^>]+>')
def remove_tags(text):
	    return tag_re.sub('', text)

css_re = re.compile(r'{[^}]+}')
def remove_css(text):
	    return css_re.sub('', text)

def remove_punctuation(line):
	return "".join(l for l in line if l not in string.punctuation)

def concat_spaces(line):
	return ' '.join(line.split())

def isURL(word):
	if word[:4] == "http":
		return True

	return False

def isJap(word):
	w = repr(word).replace("\'","")
	#if w[:2] == "\\x":
	if "\\x" in w:
		return True
	return False

def getTopWords(numWords):

	with open("stopwords") as s:
			stopwords = s.readlines()
	stopwords = ([s.strip('\n') for s in stopwords])

	f_in_msg = open(argv[1], "r")
	count_words = {}

	for line in f_in_msg:
		content = remove_tags(line).replace("@###@###@###@", " ").lower()
		content = remove_css(content)
		content = content.replace("=", " ")
		content = remove_punctuation(content)
		content = concat_spaces(content)

		aux_content = ""
		tokens = content.split(" ")
		for t in tokens:
			if t not in stopwords and len(t) > 3 and not (isURL(t)) and not (isJap(t)):
				aux_content = aux_content + t + " "

		print aux_content

		#		if t not in count_words:
		#			count_words[t] = 0
		#		count_words[t] += 1

	#top = sorted(count_words.iteritems(), key=lambda x:-x[1])[:numWords]
	#for k, v in top:
	#	print k + ": " + str(v)

getTopWords(20)
