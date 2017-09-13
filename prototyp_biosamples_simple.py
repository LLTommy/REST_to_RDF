import requests
from rdflib import Graph, plugin, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF
import json
import math
from multiprocessing import Process
import logging

listOfUnMappedKeys=[]
listOfUnMappedPropertiesType=[]

def relation(links, node, relationship, graph, context):
    rel=requests.get(links["_links"][relationship]["href"])
    reply=rel.json()
    if len(reply["_embedded"]["samplesrelations"])>0:
        for entry in reply["_embedded"]["samplesrelations"]:
            graph.add( (node, URIRef(config["relations"][relationship]), URIRef(context["base"]+entry["accession"] ) ) )

def buildGraph(params):
    context=params[0]
    filename=params[1]
    startpage=params[2]
    endpage=params[3]
    pageSize=params[4]

    output_file=open('biosamples_rdf_from_rest_'+str(filename)+'.ttl', 'w')
    page=startpage
    keep_running=True

    while keep_running:
        url=context['url']+"?size="+str(pageSize)+"&page="+str(page)
        print url
        r = requests.get(url)
        reply=r.json()
        samples=reply["_embedded"]["samples"]
        g = Graph()
        for sample in samples:
            node=URIRef(context["base"]+sample['accession'])

            g.add( (node, URIRef(config['id']), Literal(sample['accession']) ) )
            g.add( (node, URIRef(config['title']), Literal(sample['name']) ) )

            if sample['description'] is not None:
                g.add( (node, URIRef(config['description']), Literal(sample['description']) ) )

            g.add( (node, URIRef(config['updateDate']), Literal(sample['updateDate']) ) )
            g.add( (node, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), URIRef("http://rdf.ebi.ac.uk/terms/biosd/Sample") ) )

            for entry in sample['characteristics'].keys():
                bnode = BNode() #Creates a blank node
                g.add ( (node, URIRef("http://purl.obolibrary.org/obo/NCIT_C25447"), bnode ) )

                propertyType = BNode() #Creates a blank node
                propertyValue = BNode() #Creates a blank node
                g.add ( (bnode, URIRef("http://schema.org/hasPropertyName"), propertyType ) )
                g.add ( (bnode, URIRef("http://schema.org/hasPropertyValue"), propertyValue ) )


                if ('ontologyTerms' in sample['characteristics'][entry][0]):
                    g.add( (propertyValue, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), URIRef(sample['characteristics'][entry][0]['ontologyTerms'][0]) ) )

                g.add( (propertyValue, URIRef("http://www.w3.org/2000/01/rdf-schema#label"), Literal(sample['characteristics'][entry][0]['text']) ) )
                g.add( (propertyValue, URIRef("http://schema.org/propertyValue"), Literal(sample['characteristics'][entry][0]['text']) ) )


                if entry in propertyTypesConfig.keys():
                    g.add( (propertyType, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), URIRef(propertyTypesConfig[entry] ) ) )

                #Logging of unmapped entries in the properties, giving us an idea what is in there and what could be matched to ontologies
                else:
                    if entry not in listOfUnMappedPropertiesType:
                        listOfUnMappedPropertiesType.append(entry)

                g.add( (propertyType, URIRef("http://www.w3.org/2000/01/rdf-schema#label"), Literal(entry) ) )
                #g.add( (propertyType, URIRef("http://schema.org/propertyName"), Literal(sample['characteristics'][entry][0]['text']) ) )
                g.add( (propertyType, URIRef("http://schema.org/propertyName"), Literal(entry) ) )



            #####Should these things added to a blank node as well? #####
            ################### Contact
            if 'contact' in sample:
                for entry in sample['contact']:
                    if ('Name' in entry):
                        g.add( (node, URIRef(config['contact']), Literal(entry['Name']) ) )

            ################### Organisation
            if 'organization' in sample:
                for entry in sample['organization']:
                    if ('Name' in entry):
                        g.add( (node, URIRef(config['organization']), Literal(entry['Name']) ) )

            ################### Publications
            if 'publications' in sample:
                for entry in sample['publications']:
                    if ('pubmed_id' in entry):
                        #note TO SELF - WE WANT TO CHANGE THIS LATER ON TO THE SAME FORMAT THAN PUBMED gives us their data
                        g.add( (node, URIRef(config['publications']), Literal(entry['pubmed_id']) ) )


            #Keep doing this for relevant/interesting data if there is something ?? is there?

            #Logging for unmapped keys, let's see if we can find something else on the top level that we did no map yet
            for key in sample.keys():
                if key not in listOfUnMappedKeys and key not in config.keys():
                    listOfUnMappedKeys.append(key)


            #####Now let's get into relationships....
            rel = requests.get(sample['_links']['relations']["href"])
            links=rel.json()

            for key in context["relations"].keys():
                    relation(links, node, key, g, context)


        #End of FOR loop
        print listOfUnMappedKeys
        page=page+1
        if page>endpage or page%4==0: #The part after the or is just for testing and will be removed for a real run
            keep_running=False

        output=g.serialize(format='turtle')                 #We use turtle
        #output=g.serialize(format='json-ld', indent=4)     #We use json-ld
        output_file.write(output)                           #Add results to the output file

    #Close the files after exiting the while loop
    output_file.close()
    logging.error("Unmapped top level keys:")
    logging.error(listOfUnMappedKeys.encode())
    logging.error("Unmapped Properties")
    logging.error(listOfUnMappedPropertiesType.encode())








### THIS IS WHERE IT ALL STARTS ###
config={
    "url" : "https://www.ebi.ac.uk/biosamples/api/samples/",
    "base" : "http://rdf.ebi.ac.uk/resource/biosamples/sample/",
    "title" : "http://purl.org/dc/terms/title",
    "id" : "http://purl.org/dc/terms/identifier",
    "description": "http://purl.org/dc/terms/description",
    "updateDate": "http://purl.org/dc/terms/modified",
    "contact" : "http://purl.obolibrary.org/obo/NCIT_C25461",
    "organization" : "http://purl.obolibrary.org/obo/NCIT_C93874",
    "publications": "http://purl.org/dc/terms/references",
    "relations": {
        "derivedFrom" : "http://purl.org/pav/derivedFrom",
        "recuratedFrom" : "http://purl.org/pav/curatedBy",
        "sameAs" : "http://www.w3.org/2004/02/skos/core#exactMatch",
        "childOf" : "http://purl.obolibrary.org/obo/NCIT_C44235"
    }
}


propertyTypesConfig={
"sampleTitle": "http://purl.org/dc/terms/title"
}

#pav providedBy
#Question: Should all be handled through hasCharacteristic or not?
#    "organism" : "http://purl.obolibrary.org/obo/NCIT_C14250",
#    "diseaseState" : "http://www.ebi.ac.uk/efo/EFO_0000408",
#    "host" : "http://purl.obolibrary.org/obo/NCIT_C66819",
#    "sex" : "http://purl.obolibrary.org/obo/NCIT_C28421",


logging.basicConfig(filename="Biosample_creater.log", level=logging.ERROR, format='%(asctime)s - %(message)s')

numberOfParalelJobs=1
pageSize=20
rel=requests.get(config['url']+'?size='+str(pageSize))
reply=rel.json()
totalPageNumer=reply['page']['totalPages']


print "Total number of pages:"
print totalPageNumer
print totalPageNumer/numberOfParalelJobs

startpoint=0
init=[]
for i in range(1,numberOfParalelJobs+1):
    params={}
    params['run']=i
    endpoint=math.ceil(totalPageNumer/float(numberOfParalelJobs))*i
    params['start']=startpoint
    if endpoint<int(totalPageNumer):
        params['end']=int(endpoint)
    else:
        params['end']=totalPageNumer

    init.append(params)
    startpoint=int(endpoint)+1


print init
print "Let's try this"

processlist=[]
for run in init:
    parms=[]
    parms.append(config)
    parms.append(run['run'])
    parms.append(run['start'])
    parms.append(run['end'])
    parms.append(pageSize)
    p=Process(target=buildGraph, args=[parms])
    p.start()
    processlist.append(p)


print("All process started")
#Going through the process list, waiting for everything to finish
for procs in processlist:
    procs.join()
print("All finished")
