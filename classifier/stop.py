import requests
import json
import math
import string
import collections
import json
import enchant
from elasticsearch import Elasticsearch
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'tweets'
TYPE_NAME = 'tweet'
ES_CLIENT = Elasticsearch(hosts = [ES_HOST], timeout = 180)

idfdict = dict()
result = dict()
stop = set(stopwords.words('english'))
def getTermVector(total_size, doc_id):
	
	a = ES_CLIENT.termvectors(index = INDEX_NAME,
					doc_type = TYPE_NAME,
					id = doc_id,
					field_statistics = True,
					fields = ['text.raw'],
					term_statistics = True
		                )
	curr_termvec = a["term_vectors"]["text.raw"]["terms"]
	tokens = curr_termvec.keys()
	d = enchant.Dict("en_US")	

	for token in tokens:
		if d.check(token):
			token = token.lower()
			if token not in idfdict and token not in stop :
				docfreq = curr_termvec[token]["doc_freq"]
				if docfreq > 1:
					idfdict.update({token : round(math.log(float(total_size)/ (1 + docfreq)),2) })
						
				
			
		
		#[idfdict.update({token : {"tf": round(float(curr_termvec[token]["term_freq"])/len(tokens), 2), "idf": round(math.log((float(total_size))/ (1 + curr_termvec[token]["doc_freq"])),2)  }}) for token in tokens]
	#return a["_id"], temp



def scrollIndex():
	ALL_QUERY = {"query": {"match_all": {}}}
	 
	rs = ES_CLIENT.search(
		index=INDEX_NAME,
		scroll='60s',
		size=10,
		body=ALL_QUERY)

	'''data = rs['hits']['hits']
	for doc in data:
		curr_doc, curr_term_vector = getTermVector(doc['_source']['id'])'''
	
	
	scroll_size = rs['hits']['total']
	total_size = rs['hits']['total']
	count = 0
	while scroll_size:
		try:
			scroll_id = rs['_scroll_id']
			rs = ES_CLIENT.scroll(scroll_id=scroll_id, scroll='60s')
			data = rs['hits']['hits']
			count += len(data)
			print count, '\r',
			for doc in data:
				#curr_doc, curr_term_vector = 
				getTermVector(total_size, doc["_id"])
				#print curr_term_vector
				#yield curr_doc, curr_term_vector
			scroll_size = len(rs['hits']['hits'])
			
		except Exception as e:
			import traceback; traceback.print_exc()
			print e

	import pdb; pdb.set_trace()
	print 'done'
	f = open('idfs.txt', 'w')
	for word in idfdict.keys():
		print word 
		f.write(word + "	" + str(idfdict[word]) + '\n')
		




def getTfidfofTerm(term):
	temp = dict()
	tokenizer = RegexpTokenizer(r'\w+')
	words = tokenizer.tokenize(term)
	l = len(words)
	temp = collections.Counter(words)
	for i in temp.keys():
		i = i.lower()
		result[i] = float(temp[i])/l 

	for word in result.keys():
		if word in idfdict:
			result[word] = result[word]  * idfdict[word] #I was also thinking to try sth like 0.7 * result[word]  + 0.3 * idfdict[word] (just trying)
		else:
			result[word] = 0

	for word in result.keys():
		print word + "	" + str(result[word])


scrollIndex()
with open('/tmp/asd.json') as f:
    import json
    idfdict=json.load(f)

import requests
text ="RT @NobelPrize: BREAKING NEWS #NobelPrize in Physics 2016 to David Thouless, Duncan Haldane and Michael Kosterlitz https://t.co/5jw75GIjRv"
getTfidfofTerm(new_text)



