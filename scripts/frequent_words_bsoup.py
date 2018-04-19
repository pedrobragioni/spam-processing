# -*- coding=utf-8 -*-
from sys import argv
from bs4 import BeautifulSoup
import string
import re

tag_re = re.compile(r'<[^>]+>')
def remove_tags(text):
	    return tag_re.sub('', text)

css_re = re.compile(r'{[^}]+}')
def remove_css(text):
	    return css_re.sub('', text)

def remove_punctuation(line):
        a = list(string.punctuation)
        a.remove("$")
        punct = ""
        for i in a:
            punct = punct + i

       # linha = ""
       # for l in line:
       #     if l not in punct:
       #         linha += l
       #     else:
       #         linha += " "

        #return linha
	return "".join(l for l in line if l not in punct)

def concat_spaces(line):
	return ' '.join(line.split())

def hasNumbers(word):
    if len(word) > 0:
        if word[0] == "$":
            return False

    for c in word:
        if c.isdigit():
            return True

    return False

    #return any(char.isdigit() for char in word)

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
        f_in_header = open(argv[2], "r")

        f_out_msg = open(argv[3], "w")
        f_out_header = open(argv[4], "w")
        count_words = {}

	for line in f_in_msg:
            line_header = f_in_header.readline()

            #try:
            soup = BeautifulSoup(line, 'html.parser')
            for elem in soup.findAll(['script', 'style']):
                elem.extract()

            text = soup.get_text()
            content = text.encode('utf-8')
            content = content.replace("@###@###@###@", " ").lower()
            #content = remove_tags(text).replace("@###@###@###@", " ").lower()
            #content = remove_css(content)
            content = content.replace("=", " ")
            content = remove_punctuation(content)
            content = concat_spaces(content)

            aux_content = ""
            tokens = content.split(" ")
            numW = 0
            for t in tokens:
                    if not hasNumbers(t) and  t not in stopwords and len(t) > 3 and not (isURL(t)) and not (isJap(t)):
                            aux_content = aux_content + t + " "
                            numW += 1

            if(numW > 3):
                f_out_msg.write(aux_content + "\n")
                f_out_header.write(line_header) 


        f_in_msg.close()
        f_in_header.close()
        f_out_msg.close()
        f_out_header.close()
            #except:
            #    print "ERROR"

		#		if t not in count_words:
		#			count_words[t] = 0
		#		count_words[t] += 1

	#top = sorted(count_words.iteritems(), key=lambda x:-x[1])[:numWords]
	#for k, v in top:
	#	print k + ": " + str(v)

getTopWords(20)
