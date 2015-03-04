import os
from os.path import join
import re 
import elasticsearch
from elasticsearch import client
from elasticsearch.client.cat import CatClient
from Term import Term

es = elasticsearch.Elasticsearch("localhost:9200", timeout=600, maxRetry=2, revival_delay=0)
index = elasticsearch.client.IndicesClient(es)
catClient = elasticsearch.client.CatClient(es)

def deleteIndex ():
    index.delete('*')

def createIndex():
        index.create(index='ap_dataset',
                 body={
                          "settings": {
                            "index": {
                              "store": {
                                "type": "default"
                              },
                              "number_of_shards": 1,
                              "number_of_replicas": 1
                            },
                            "analysis": {
                              "analyzer": {
                                "my_english": { 
                                  "type": "english",
                                  "stopwords_path": "stoplist.txt" 
                                }
                              }
                            }
                          }
                        })
        
        index.put_mapping(index='ap_dataset', doc_type = 'document', body={
                                                      "document": {
                                                        "properties": {   
                                                        "docno": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                          },
                                                        "text": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "analyzed",
                                                            "term_vector": "with_positions_offsets_payloads",
                                                            "analyzer": "my_english"
                                                          }
                                                        }
                                                      }
                                                    })

    
def generateListOfStopWords():
    listOfStopWords = []
    stopWordsPath = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/stoplist.txt"
    queryFile = open(stopWordsPath, "r")
    for line in queryFile:
        listOfStopWords.append(line.strip())
    return listOfStopWords
    
def readDocumentList():
    
    listOfStopWords = generateListOfStopWords()
    path = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/ap89_collection/"
    listOfFiles = os.listdir(path);
    documentIds = []
    j = 0
    for file in listOfFiles:
        i = 0
        f = open(join(path, file), "r").read()
        doc = re.findall('<DOC>.*?</DOC>', f, re.DOTALL)
        documentNumbers = getDocNo(f)
        documentIds = documentIds + documentNumbers
        documentIds.pop()
        corpusTextList = []
        for d in doc:
            mergedStr = ""
            corpusTextContent = getTextInfo(d)                
            if len(corpusTextContent) > 1:   
                mergedStr = (mergeTwoTextTags(corpusTextContent, mergedStr))
                addDocumentToIndex(documentNumbers[i], mergedStr)
            else:
                corpusTextContentString = corpusTextContent[0].strip()
                addDocumentToIndex(documentNumbers[i], corpusTextContentString)
            i = i + 1
            
def addDocumentToIndex(docId, corpusContent):

    es.index(index='ap_dataset', doc_type='document', id=docId, body={
            'text': corpusContent,'docno':docId})

def mergeTwoTextTags(corpusTextContent, mergedStr):
    for str in corpusTextContent:
        str = str.strip()
        mergedStr = mergedStr + str
    return mergedStr
    
def getTextInfo(d):
    listOfRemovedTags = []
    text = re.findall('<TEXT>.*?</TEXT>', d, re.DOTALL)
    for elem in text:
        removedTagString = re.sub('<.*?>', '', elem)
        listOfRemovedTags.append(removedTagString)
    return listOfRemovedTags

def getDocNo(f):
    docNo = re.findall('<DOCNO>.*</DOCNO>', f)
    listOfRemovedTags = []
    for d in docNo:
        removedTagString = re.sub('<.*?>', '', d)
        removedTagString = removedTagString.strip()
        listOfRemovedTags.append(removedTagString)
    return listOfRemovedTags


def main():
    deleteIndex()
    createIndex()
    readDocumentList()
    
main()