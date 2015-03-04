import os
from os.path import join
import re 
import elasticsearch
from elasticsearch import client
from elasticsearch.client.cat import CatClient
import json
from Term import Term
import string
import math


es = elasticsearch.Elasticsearch("localhost:9200", timeout=600, maxRetry=2, revival_delay=0)
index = elasticsearch.client.IndicesClient(es)
catClient = elasticsearch.client.CatClient(es)


def generateListOfStopWords():
    listOfStopWords = []
    stopWordsPath = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/stoplist.txt"
    queryFile = open(stopWordsPath, "r")
    for line in queryFile:
        listOfStopWords.append(line.strip())
    return listOfStopWords

def queryFromFile():
    path = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/query_desc.51-100.short.txt"
    queryFile = open(path, "r")
    listOfStopWords = generateListOfStopWords()
    for line in queryFile:
        queryTFDict = {}
        queryTerms = line.split()
        for x in range(0, 3):
            queryTerms.pop(1)
        
        queryTermsWithoutStopWords = [term for term in queryTerms if term not in listOfStopWords]
        
        for term in queryTermsWithoutStopWords:
            if term.startswith('"') and term.endswith('"'):
                term = term[1:-1]
            if queryTFDict.has_key(term):
                
                queryTFDict[term] = queryTFDict[term] + 1
            else:    
                queryTFDict[term] = 1
        
        print queryTermsWithoutStopWords
        #queryElasticSearch(queryTermsWithoutStopWords, queryTFDict)
        #queryElasticSearchForSmoothing(queryTermsWithoutStopWords, queryTFDict)
        
        queryElasticSearchForJelinek(queryTermsWithoutStopWords, queryTFDict)

            
def queryElasticSearch(queryTermsWithoutStopWords, queryTFDict):
    queryNo = queryTermsWithoutStopWords.pop(0)[:-1]
    countOfDocuments = es.count(index='ap_dataset')
    lengthDict = storeLengthOfDocumentsInDictionary()
    tfList = []
    dict = {}    
    print queryTermsWithoutStopWords
    for eachTerm in queryTermsWithoutStopWords:
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
        results = []
        results = es.search(index='ap_dataset', doc_type='document', body='{"query" : {"match" : {"text" :' + ' "' + eachTerm + '"' + '}}, "explain" : true}', analyzer='snowball', size=countOfDocuments['count'])
        print len(results['hits']['hits'])
        listOfHitsIds = []
        for doc in results['hits']['hits']:
            listOfHitsIds.append(doc['_id'])
            d = json.dumps(doc['_explanation'])
            x = d.find("termFreq")
            y = d.find("docFreq")
            z = d.find("maxDocs")
            tf = d[x:x + 13]
            df = d[y:z - 1]
            tf = tf[9:-1]
            df = df[8:-1]
            okapiTfForAllTerms (float(tf) , 164.788941637733531 , float(lengthDict[doc['_id']]) , doc['_id'], dict)
            #tfIdforAllTerms (float(tf) , 164.788941637733531 , float(lengthDict[doc['_id']]) , doc['_id'] , dict, float(df))   
            #okapiBm25 (float(tf) , 164.788941637733531 , float(lengthDict[doc['_id']]) , doc['_id'], dict, float(df), float(queryTFDict[eachTerm]))
               
        print eachTerm + " Finished"
    writeQueryModelsToFile(dict, queryNo,"okapiTest.txt",100)
    print queryNo + " Finished"
    
def queryElasticSearchForSmoothing(queryTermsWithoutStopWords, queryTFDict):
    queryNo = queryTermsWithoutStopWords.pop(0)[:-1]
    print queryNo
    countOfDocuments = es.count(index='ap_dataset')
    lengthDict = storeLengthOfDocumentsInDictionary()
    tfList = []
    listOfDicts=[]
    smoothingDict = storeDictionaryIds()
    print queryTermsWithoutStopWords
    for eachTerm in queryTermsWithoutStopWords:
        dict = {}
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
        results = []
        results = es.search(index='ap_dataset', doc_type='document', body='{"query" : {"match" : {"text" :' + ' "' + eachTerm + '"' + '}}, "explain" : true}', analyzer='snowball', size=countOfDocuments['count'])
        print len(results['hits']['hits'])
        listOfHitsIds = []
        for doc in results['hits']['hits']:
            listOfHitsIds.append(doc['_id'])
            d = json.dumps(doc['_explanation'])
            x = d.find("termFreq")
            y = d.find("docFreq")
            z = d.find("maxDocs")
            tf = d[x:x + 13]
            df = d[y:z - 1]
            tf = tf[9:-1]
            df = df[8:-1]
            
            unigramLMLaplaceSmoothing(float(tf) ,  189327 ,lengthDict[doc['_id']] , doc['_id'], dict)

        listOfDicts.append(dict)
        
        print eachTerm + " Finished"
    
    for key in smoothingDict:
        for eachDict in listOfDicts:
            if eachDict.has_key(key):
                smoothingDict[key] = smoothingDict[key] + eachDict[key]
            else:
                p_laplace = 1.0 / (lengthDict[key] + 189327)
                lm_lapace_zero = math.log10(p_laplace)
                smoothingDict[key] = smoothingDict[key] + lm_lapace_zero
    
    writeQueryModelsToFile(smoothingDict, queryNo, "laplaceSmoothing.txt",100)
    print queryNo + " Finished"        
    
def queryElasticSearchForJelinek(queryTermsWithoutStopWords, queryTFDict):
    queryNo = queryTermsWithoutStopWords.pop(0)[:-1]
    countOfDocuments = es.count(index='ap_dataset')
    lengthDict = storeLengthOfDocumentsInDictionary()
    tfList = []
    ttfDict = {}
    listOfDicts=[]
    smoothingDict = storeDictionaryIds()
    print queryTermsWithoutStopWords
    for eachTerm in queryTermsWithoutStopWords:
        dict = {}
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
        results = []
        results = es.search(index='ap_dataset', doc_type='document', body='{"query" : {"match" : {"text" :' + ' "' + eachTerm + '"' + '}}, "explain" : true}', analyzer='snowball', size=countOfDocuments['count'])
        print len(results['hits']['hits'])
        listOfHitsIds = []
        ttf = 0.0
        for doc in results['hits']['hits']:
            listOfHitsIds.append(doc['_id'])
            d = json.dumps(doc['_explanation'])
            x = d.find("termFreq")
            tf = d[x:x + 13]
            tf = tf[9:-1]
            ttfDict[eachTerm] = float(tf)             
            ttf = ttf + float(tf)
        
        for doc in results['hits']['hits']:
            listOfHitsIds.append(doc['_id'])
            d = json.dumps(doc['_explanation'])
            x = d.find("termFreq")
            y = d.find("docFreq")
            z = d.find("maxDocs")
            tf = d[x:x + 13]
            df = d[y:z - 1]
            tf = tf[9:-1]
            df = df[8:-1]
            unigramLMJelinek(float(tf),13953998,lengthDict[doc['_id']],doc['_id'], dict, ttf,eachTerm)
        
        listOfDicts.append(dict)
        print eachTerm + " Finished"
    
    for key in smoothingDict:
        for eachDict in listOfDicts:
            if eachDict:
                if eachDict.values()[0].has_key(key):
                    value = eachDict.values()[0]
                    smoothingDict[key] = smoothingDict[key] + value[key]
                else:
                    termKey = eachDict.keys()[0]
                    p_jm = 0.5 *( ttfDict[termKey] / 13953998)
                    lm_jm_zero = math.log10(p_jm)
                    smoothingDict[key] = smoothingDict[key] + lm_jm_zero
    
    writeQueryModelsToFile(smoothingDict, queryNo, "jelinekMercerSmoothing500.txt",500)
    print queryNo + " Finished"
    
def writeQueryModelsToFile(dict, queryNo, filename,topDocuments):
    f = open(filename , "a")
    i = 1
    for doc in sorted(dict, key=dict.get, reverse=True):
        qrelString = queryNo + " Q0 " + doc + " " + str(i) + " " + str(dict[doc]) + " " + "Exp" 
        f.write(qrelString + "\n")
        if (i == topDocuments):
            break
        i = i + 1
        
def okapiBm25(tf, avgLengthDocument, lengthDocument, docId, dict, df, tfq):
    k1 = 1.2
    k2 = 100.0
    b = 0.75
    
    c1 = math.log10((84678 + 0.5) / (df + 0.5))
    c2 = (tf + (k1 * tf)) / (tf + k1 * ((1 - b) + (b * (lengthDocument / avgLengthDocument))))
    c3 = (tfq + (k2 * tfq)) / tfq + k2
    
    bm25 = c1 * c2 * c3
    
    if (dict.has_key(docId)):
        dict[docId] = dict[docId] + bm25
    else:
        dict[docId] = bm25

def okapiTfForAllTerms(tf, avgLengthDocument, lengthDocument, docId, dict):
    
    denom = tf + 0.5 + (1.5 * (lengthDocument / avgLengthDocument))
    okapiTfForATerm = tf / denom
    
    if (dict.has_key(docId)):
        dict[docId] = dict[docId] + okapiTfForATerm
    else:
        dict[docId] = okapiTfForATerm

def tfIdforAllTerms(tf, avgLengthDocument, lengthDocument, docId, dict, df):
   
    okapiTfForATerm = tf / (tf + 0.5 + 1.5 * (lengthDocument / avgLengthDocument))
    
    tfIdf = okapiTfForATerm * math.log10(84678 / df) 
    
    if (dict.has_key(docId)):
        tfIdfScore = dict.get(docId)
        newTfIdfScore = tfIdfScore + tfIdf
        dict[docId] = newTfIdfScore
    else:
        dict[docId] = tfIdf

def unigramLMLaplaceSmoothing(tf, vocabulary, lengthDocument, docId, dict):
    p_laplace = (tf + 1.0) / (float(lengthDocument) + vocabulary)
    lm_laplace = math.log10(p_laplace)
    dict[docId] = lm_laplace

def unigramLMJelinek(tf,totalDocumentLength,lengthDocument,docId,dict,ttf,eachTerm):
    l = 0.5
    c1 = l * (tf / lengthDocument)
    c2 = (ttf - tf) / (totalDocumentLength - lengthDocument)
    p_jm = c1 + ((1 - l)*c2)
    lm_jm = math.log10(p_jm)
    if dict.has_key(eachTerm):
        dict[eachTerm][docId] = lm_jm
    else:
        dict[eachTerm] = {eachTerm : lm_jm}
        
def findLengthOfAllDocumentFromElasticSearch():
    results = es.search(index='ap_dataset', doc_type='document', body='{"query" : {"match_all" : {}}}', size=84678)
    documentIds = [doc['_id'] for doc in results['hits']['hits']]
    print len(documentIds)
    f = open("lengthOfDocuments.txt", "a")
    for id in documentIds:
        results = es.search(index='ap_dataset', doc_type='document', body = {
                                                                        "query": {
                                                                        "match": {
                                                                           "docno": id
                                                                        }},
                                                                        "facets": {
                                                                                "text": {
                                                                                    "statistical": {
                                                                                        "script": "doc['text'].values.size()"
                                                                                    }
                                                                                }
                                                                            }
                                                                    }, size=1)
        
        f.write(id + ' ' + str(results['facets']['text']['total']) + '\n')

def storeLengthOfDocumentsInDictionary():
    file = open("lengthOfDocuments.txt", "r")
    dict = {}
    for line in file:
        listOfLengthDocuments = line.split();
        dict[listOfLengthDocuments[0]] = float(listOfLengthDocuments[1])
    return dict

def storeDictionaryIds():
    file = open("lengthOfDocuments.txt", "r")
    dict = {}
    for line in file:
        listOfLengthDocuments = line.split();
        dict[listOfLengthDocuments[0]] = 0.0
    return dict
    

def main():
    queryFromFile()
        
main()
