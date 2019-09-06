import requests
from rdflib import Graph, plugin, Namespace, URIRef, Literal, BNode
#from rdflib.namespace import RDF
#import json
import math
from multiprocessing import Process
import logging
import configparser

listOfUnMappedKeys=[]
unmapped_properties=set()

#Not used atm, but shouldn't it be? Should we reactivate this part?
#def relation(links, node, relationship, graph, context):
    #rel=requests.get(links["_links"][relationship][relation("href"], headers)
    #reply=rel.json()
    #if len(reply["_embedded"]["samplesrelations"])>0:
    #    for entry in reply["_embedded"]["samplesrelations"]:
    #        graph.add( (node, URIRef(config["relationship"][relationship]), URIRef(context["base"]+entry["accession"] ) ) )
#    print(key)
#    print(context[key])
#    graph.add( (node, URIRef(config["relationship"][relationship]), URIRef(context["base"]+entry["accession"] ) ) )


def buildGraph(params):
    context=params[0]
    jobnumber=params[1]
    startpage=params[2]
    endpage=params[3]
    pageSize=params[4]

    page=startpage
    keep_running=True

    while keep_running:
        url=context['apiurl']+"?size="+str(pageSize)+"&page="+str(page)
        r = requests.get(url, headers)
        reply=r.json()

        samples=reply["_embedded"]["samples"]
        g = Graph()
        for sample in samples:
            node=URIRef(context["base"]+sample['accession'])

            g.add( (node, URIRef(context['id']), Literal(sample['accession']) ) )
            g.add( (node, URIRef(context['title']), Literal(sample['name']) ) )

            #No more description field in new biosamples API?
            #if sample['description'] is not None:
            #    g.add( (node, URIRef(context['description']), Literal(sample['description']) ) )

            g.add( (node, URIRef(context['releasedate']), Literal(sample['releaseDate']) ) )
            g.add( (node, URIRef(context['updatedate']), Literal(sample['updateDate']) ) )
            g.add( (node, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), URIRef("http://rdf.ebi.ac.uk/terms/biosd/Sample") ) )

            if 'characteristics' in sample:
                characteristics_dict = sample['characteristics']
                for characteristic_key, characteristic_values in characteristics_dict.items():

                    # the value in the dict is always an array of length 1. Who knows why...? We will have to test if that is always true for everything...?
                    # e.g.
                    #
                    # strain": [
                    #  {
                    #    "text": "JCM 9152"
                    #    }
                    # ],
                    characteristic_value = characteristic_values[0]
                    
                    attribute_node = BNode() #Creates a blank node

                    g.add ( (node, URIRef("http://semanticscience.org/resource/SIO_000008"), attribute_node ) ) #OR should it be http://purl.obolibrary.org/obo/NCIT_C25447 ?

                    if characteristic_key in propertyTypesConfig.keys():
                        propertyType = URIRef(propertyTypesConfig[characteristic_key] )
                    else:
                        propertyType = BNode()
                        g.add( (propertyType, URIRef("http://www.w3.org/2000/01/rdf-schema#label"), Literal(characteristic_key, lang='en') ) )
                        
                    if ('ontologyTerms' in characteristic_value):
                        # it seems that this is always an array of length 1
                        propertyValue = URIRef(characteristic_value['ontologyTerms'][0])
                    else:
                        propertyValue = BNode() #Creates a blank node
                        g.add( (propertyValue, URIRef("http://www.w3.org/2000/01/rdf-schema#label"), Literal(characteristic_value['text'], lang='en') ) )
                        
                    g.add ( (attribute_node, URIRef("https://w3id.org/biolink/vocab/has_attribute_type"), propertyType ) )
                    g.add ( (attribute_node, URIRef("https://w3id.org/biolink/vocab/has_qualitative_value"), propertyValue ) )

                    #Logging of unmapped entries in the properties, giving us an idea what is in there and what could be matched to ontologies
                    unmapped_properties.update(sample.keys())



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
                        g.add( (node, URIRef(config['publications']), Literal(entry['pubmed_id']) ) )

            ################### ExternalReferences
            if 'externalReferences' in sample:
                for entry in sample['externalReferences']:
                    if ('name' in entry):
                        g.add( (node, URIRef(config['organization']), Literal(entry['name']) ) )
                    if ('url' in entry):
                        g.add( (node, URIRef(config['url']), Literal(entry['url']) ) )


            #Keep doing this for relevant/interesting data if there is something ?? is there?

            #Logging for unmapped keys, let's see if we can find something else on the top level that we did no map yet
            for sample_key in sample.keys():
                if sample_key not in listOfUnMappedKeys and sample_key not in config.keys():
                    listOfUnMappedKeys.append(sample_key)


# DO WE CARE about relationships???
#            if 'relationships' in sample:
#                for rel_key in sample['relationships']:
#                    print(rel_key)
#                    print(config['relationships'].keys())
#                    if rel_key in config['relationships'].keys():
#                        g.add( (URIRef(context["base"]+rel_key['source']), URIRef(config['relationships'][rel_key['type']]), URIRef(context["base"]+rel_key['target']) ) )
#                    else:
#                        print("Missing in the config file! "+str(rel_key)+" Thus I can not assign this relationship")
            #####Now let's get into relationships....
            #rel = requests.get(sample['_links']['relations']["href"])
            #links=rel.json()
            #for key in context["relationships"].keys():
            #    relation(links, node, key, g, context)


        #End of FOR loop
        page=page+1
        if page>endpage or page%4==0: #The part after the or is just for testing and will be removed for a real run
            keep_running=False

    g.serialize(destination="rdf_export_job_"+str(jobnumber)+".ttl", format='turtle')                 #We use turtle
    logging.error("Unmapped top level keys:")
    logging.error(listOfUnMappedKeys)
    logging.error("Unmapped Properties")
    logging.error(unmapped_properties)


################ THIS IS WHERE IT ALL STARTS ################
numberOfParalelJobs=1       # How many parallel jobs we want to run?
pageSize=500                # What is the pagesize of the API request we want to look at?

parser=configparser.RawConfigParser()
parser.read("config_file.ini")

basics=parser.items('Basics')
###relationships=parser.items('relationships') # DO we care about relationships?    ## DO we care about relationships?
propertyTypes=parser.items('propertyTypes')

config={}
for entry in basics:
    config[entry[0]]=entry[1]

#### #DO we care about the relationships?
#relationshipConfig={}
#for entry in relationships:
#    relationshipConfig[entry[0]]=entry[1]
#config["relationships"]=relationshipConfig

propertyTypesConfig={}

propertyTypes=parser.items('propertyTypes')
for entry in propertyTypes:
    propertyTypesConfig[entry[0]]=entry[1]


logging.basicConfig(filename="Biosamples_crawler.log", level=logging.INFO, format='%(asctime)s - %(message)s')
headers={'Accept': 'application/hal+json'}

rel=requests.get(config['apiurl']+'?size='+str(pageSize), headers)
reply=rel.json()
totalPageNumber=reply['page']['totalPages']

print("Total number of pages is "+str(totalPageNumber)+" and page per job "+str(totalPageNumber/numberOfParalelJobs))

startpoint=0
init=[]
for i in range(1,numberOfParalelJobs+1):
    params={}
    params['run']=i
    endpoint=math.ceil(totalPageNumber/float(numberOfParalelJobs))*i
    params['start']=startpoint
    if endpoint<int(totalPageNumber):
        params['end']=int(endpoint)
    else:
        params['end']=totalPageNumber

    init.append(params)
    startpoint=int(endpoint)+1


processlist=[]
for run in init:
    params=[]
    params.append(config)
    params.append(run['run'])
    params.append(run['start'])
    params.append(run['end'])
    params.append(pageSize)
    p=Process(target=buildGraph, args=[params])
    p.start()
    processlist.append(p)


print("All process started")
#Going through the process list, waiting for everything to finish
for procs in processlist:
    procs.join()
print("All finished")
