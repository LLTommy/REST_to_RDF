import requests
from rdflib import Graph, plugin, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF
import json
import math
from multiprocessing import Process

map2=[]
unmapped=[]

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

    output_file=open('biosamples_rdf_from_rest_'+str(filename)+'.ttl', 'w')
    page=startpage
    keep_running=True

    while keep_running:
        page=page+1
        if page>endpage:
            keep_running=False

        print context['url']+"?size=500&page="+str(page)
        r = requests.get(context['url']+"?&page="+str(page))
        reply=r.json()
        samples=reply["_embedded"]["samples"]
        g = Graph()
        listOfUnMappedKeys=[]
        for sample in samples:
            node=URIRef(context["base"]+sample['accession'])

            g.add( (node, URIRef(config['id']), Literal(sample['accession']) ) )
            g.add( (node, URIRef(config['title']), Literal(sample['name']) ) )

            if sample['description'] is not None:
                g.add( (node, URIRef(config['description']), Literal(sample['description']) ) )

            g.add( (node, URIRef(config['updateDate']), Literal(sample['updateDate']) ) )
            g.add( (node, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), URIRef("http://purl.obolibrary.org/obo/OBI_0000747") ) )

            for entry in sample['characteristics'].keys():
                bnode = BNode() #Creates a blank node
                g.add ( (node, URIRef("http://rdf.hasCharacteristic"), bnode ) )

                if ('ontologyTerms' in sample['characteristics'][entry][0]):
                    g.add( (bnode, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), URIRef(sample['characteristics'][entry][0]['ontologyTerms'][0]) ) )

                g.add( (bnode, URIRef("http://rdf.propertyType"), Literal(entry) ) )
                g.add( (bnode, URIRef("http://rdf.propertyValue"), Literal(sample['characteristics'][entry][0]['text']) ) )
                g.add( (bnode, URIRef("http://www.w3.org/2000/01/rdf-schema#label"), Literal(entry+"/"+sample['characteristics'][entry][0]['text']) ) )


            #####Should these things added to a blank node as well? #####
            ################### Contact
            if 'contact' in sample:
                for entry in sample['contact']:
                    if ('Name' in entry):
                        g.add( (node, URIRef(config['contact']), Literal(entry['Name']) ) )
    #                else:
    #                    print "Did not find Name in contact in "+sample["accession"]
    #        else:
    #           print "No contact in "+sample["accession"]


            ################### Organisation
            if 'organization' in sample:
                for entry in sample['organization']:
                    if ('Name' in entry):
                        g.add( (node, URIRef(config['organization']), Literal(entry['Name']) ) )
    #                else:
    #                    print "Did not find Name in organisation in "+sample["accession"]
    #        else:
    #            print "No organization in "+sample["accession"]


            ################### Publications
            if 'publications' in sample:
                for entry in sample['publications']:
                    if ('pubmed_id' in entry):
                        #note TO SELF - WE WANT TO CHANGE THIS LATER ON TO THE SAME FORMAT THAN PUBMED gives us their data
                        g.add( (node, URIRef(config['publications']), Literal(entry['pubmed_id']) ) )
    #                else:
    #                    print "Did not find pubmed_id in publication in "+sample["accession"]
    #        else:
    #            print "No publication in "+sample["accession"]

            #Keep doing this for relevant/interesting data if there is something ?? is there?


            #####Now let's get into relationships....
            rel = requests.get(sample['_links']['relations']["href"])
            links=rel.json()

            for key in context["relations"].keys():
                    relation(links, node, key, g, context)

        #End of FOR loop
        output=g.serialize(format='turtle')                 #We use turtle
        #output=g.serialize(format='json-ld', indent=4)     #We use json-ld
        output_file.write(output)                           #Add results to the output file

    #Close the files after exiting the while loop
    output_file.close()








### THIS IS WHERE IT ALL STARTS ###
config={
    "url" : "https://www.ebi.ac.uk/biosamples/api/samples/",
    "base" : "http://rdf.ebi.ac.uk/resource/biosamples/sample/",
    "title" : "http://purl.org/dc/terms/title",
    "id" : "http://edamontology.org/data_0842",
    "description": "http://semanticscience.org/resource/SIO_000136",
    "updateDate": "http://purl.obolibrary.org/obo/FLU_0000786",
    "contact" : "http://needsBetterTerm/contact",
    "organization" : "http://needsBetterTerm/organization",
    "publications": "http://purl.obolibrary.org/obo/IAO_0000311",
    "relations": {
        "derivedFrom" : "http://purl.org/dsw/derivedFrom",
        "recuratedFrom" : "http://recuratedFromBetterTerm",
    }
}


numberOfParalelJobs=5

rel=requests.get(config['url']+'?size=500')
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
    p=Process(target=buildGraph, args=[parms])
    p.start()
    processlist.append(p)


print("All process started")
#Going through the process list, waiting for everything to finish
for procs in processlist:
    procs.join()
print("All finished")
