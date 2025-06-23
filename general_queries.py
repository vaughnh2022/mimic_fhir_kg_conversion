from pymongo import MongoClient
import json 
#import langcodes
import networkx as nx
import time
import sys
import argparse
from pathlib import Path
from rdflib import Graph
from rdflib.exceptions import ParserError
import logging

# Connect to a local MongoDB instance (default port 27017)
client = MongoClient("mongodb://localhost:27017/")

# Access a database
db = client["mimic"]
collection = db["fhir"]


#comment out any entity functions you do not want in the final script for test purposes
#it takes roughly a minute to create the full script
def create_ttl_script():
    time_start = time.time()
    with open("final_script.ttl", "w") as f:
        f.write("")
    print("writing to file")
    with open('mimic_fhir_script.ttl', 'r', encoding='utf-8') as file:
        ttl_string = file.read()
    with open("final_script.ttl", "w") as f:
        f.write(ttl_string)
    create_organization_entities()
    create_location_entities()
    create_patient_entities()
    create_encounter_entities()
    create_procedure_entities()
    create_condition_entities()
    create_medicationDispense_entities()
    create_medicationRequest_entities()
    create_specimen_entities()
    create_medication_entities()
    create_medicationAdministration_entities()
    create_observation_entities()
    char_count = 0
    line_count = 0
    with open("final_script.ttl", "r", encoding="utf-8") as f:
        for line in f:
            line_count += 1
            char_count += len(line)
    time_end = time.time()
    print(f"Script completed in {time_end - time_start:.4f} seconds")   
    print(f"Character count: {char_count}")
    print(f"Line count: {line_count}")


#database specific functions
#these output to the terminal

def get_sample_from_resource_type(resource_type):
    # Find a document that contains the category code
    sample = collection.find_one({
        "resourceType": resource_type
    })
    print(json.dumps(sample, indent=2, default=str))
    return sample

def get_resource_type_list():
    pipeline = [
        {
            "$group": {
                "_id": "$resourceType",
                "count": { "$sum": 1 }
            }
        },
        {
            "$sort": { "count": -1 }
        }
    ]
    
    results = collection.aggregate(pipeline)
    for result in results:
        print(f"{result['_id']}, Count: {result['count']}")

def get_fields_in_all_documents():
    total_docs = collection.count_documents({})
    pipeline = [
    {
        "$project": {
            "fields": { "$objectToArray": "$$ROOT" }
        }
    },
    {
        "$unwind": "$fields"
    },
    {
        "$group": {
            "_id": "$fields.k",
            "count": { "$sum": 1 }
        }
    },
    {
        "$match": {
            "count": total_docs  # Only keep fields that appear in all docs
        }
    }
    ]

    # Step 3: Run pipeline and print results
    fields_in_all_docs = collection.aggregate(pipeline)
    for x in fields_in_all_docs:
        print(x)

def get_distinct_fields(resource_type):
    def extract_all_paths(doc, prefix=""):
        paths = set()
        if isinstance(doc, dict):
            for key, value in doc.items():
                current_path = f"{prefix}.{key}" if prefix else key
                paths.add(current_path)
            
                if isinstance(value, (dict, list)):
                    paths.update(extract_all_paths(value, current_path))
                    
        elif isinstance(doc, list):
            for i, item in enumerate(doc):
                current_path = f"{prefix}.{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    paths.update(extract_all_paths(item, current_path))
                    
        return paths

    documents = collection.find({"resourceType": resource_type})
    
    field_counts = {}
    total_docs = 0
    
    for doc in documents:
        total_docs += 1
        paths = extract_all_paths(doc)
        for path in paths:
            field_counts[path] = field_counts.get(path, 0) + 1

    print(f"Field analysis for resourceType: {resource_type}")
    print(f"Total documents analyzed: {total_docs}")
    print("-" * 60)
    
    for field_path in sorted(field_counts.keys()):
        count = field_counts[field_path]
        percentage = (count / total_docs * 100) if total_docs > 0 else 0
        print(f"{field_path:<40} | Count: {count:>4} | {percentage:>5.1f}%")

def collect_one_pipeline():
    #
    result = collection.find_one({"resourceType":"Observation","valueString":{"$exists":True}})

    print(json.dumps(result, indent=2, default=str))


#file writing and sanatization functions

def write_to_middle(insertion):
    with open("middle_man.txt", "a") as f:
            f.write(insertion)

def clear_middle():
    with open("middle_man.txt", "w") as f:
            f.write("")

def move_to_final():
    with open("middle_man.txt", "r", encoding="utf-8") as txt_file:
        content = txt_file.read()

    with open("final_script.ttl", "a", encoding="utf-8") as ttl_file:
        ttl_file.write("\n")
        ttl_file.write(content)
    clear_middle()

def fhir_exists(word):
    FHIR_OWL_URL = "http://hl7.org/fhir/fhir.ttl"

    # Define FHIR namespace
    FHIR = Namespace("http://hl7.org/fhir/")

    # Create RDF graph
    g = Graph()
    g.parse(FHIR_OWL_URL, format="turtle")

    # Define the property to check
    property_to_check = FHIR[word]

    # Check if it exists as a property in the ontology
    exists = (property_to_check, None, None) in g or (None, None, property_to_check) in g

    # Print result
    if exists:
        print(f"{property_to_check} exists in the FHIR ontology.")
    else:
        print(f"{property_to_check} is NOT defined in the FHIR ontology.")

def split_refrence(refrence):
    return refrence.split("/")[-1]

def escape_turtle_string(s):
    return s.replace('\\', '\\\\').replace('"', '\\"')

def get_meta(meta):
    if not meta:
        return ""
    meta_version = meta.get('versionId', '')
    meta_lastUpdated = meta.get('lastUpdated', '')
    meta_source = meta.get('source', '')
    meta_profiles = meta.get('profile', [])
    
    # If meta_profiles is a list, join it into a clean string without brackets
    if isinstance(meta_profiles, list):
        clean_profiles = ", ".join(meta_profiles)
    else:
        clean_profiles = str(meta_profiles)
    clean_profiles.strip("[]'\"")
    
    meta_line = f"""fhir:meta [
                fhir:versionId [ fhir:v "{meta_version}" ] ;
                fhir:lastUpdated [ fhir:v "{meta_lastUpdated}"^^xsd:dateTime ] ;
                fhir:source [ fhir:v "{meta_source}" ] ;
                fhir:profile [ fhir:v "{clean_profiles}" ]
            ] ;"""
    return meta_line

def sanatize_quotes(s):
     return s.replace('"', '\\"')

def get_coding(coding):
    if len(coding)==0:
        return ""
    typeCodingSystem = coding[0].get('system', '')
    typeCodingCode = coding[0].get('code', '')
    typeCodingDisplay = coding[0].get('display', '')
    coding_line =f"""\t\t\t\tfhir:coding [
                    fhir:system  [ fhir:v "{typeCodingSystem}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{typeCodingCode}" ] ;
                    fhir:display [ fhir:v "{typeCodingDisplay}" ]
                ] """
    return coding_line

def get_small_coding(coding):
    if len(coding)==0:
        return ""
    typeCodingSystem = coding[0].get('system', '')
    typeCodingCode = coding[0].get('code', '')
    coding_line =f"""fhir:coding [
                        fhir:system  [ fhir:v "{typeCodingSystem}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{typeCodingCode}" ] 
                    ] """
    return coding_line

def get_identifier(identifier):
    if len(identifier)==0:
        return ""
    identifierSystem=identifier[0].get('system',"")  if identifier else ""
    identifierValue=identifier[0].get('value',"")  if identifier else ""
    identifier_line=f"""fhir:identifier [
                fhir:system [ fhir:v "{identifierSystem}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{sanatize_quotes(identifierValue)}" ]
            ] ;"""
    return identifier_line
    
def sanitize_for_kg_literal(text):
    """
    Escapes a string to be safely inserted as a literal in a FHIR RDF Turtle knowledge graph.
    Escapes: double quotes, backslashes, newlines, carriage returns.
    """
    if not isinstance(text, str):
        text = str(text)

    # Escape backslashes first
    text = text.replace('\\', '\\\\')
    # Escape double quotes
    text = text.replace('"', '\\"')
    # Escape newlines and carriage returns
    text = text.replace('\n', '\\n').replace('\r', '\\r')

    return text


#these functions create the entities

def create_organization_entities():
    time_start = time.time()
    print("creating organization entities")
    pipeline = [
        {"$match":{"resourceType":"Organization"}},
        {"$project":{"_id":1,"id":1,"identifier":1,"active":1,"type":1,"name":1,"meta":1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID=result.get('id')
        identifier = get_identifier(result.get('identifier', []))
        name=result.get('name')
        active=result.get('active')
        meta = get_meta(result.get('meta', {}))
        type_list = result.get('type', [])
        coding_list=get_coding(type_list[0].get('coding', [])) if type_list else []
        insert =f"""se:{fhirID} a fhir:Organization ;
            fhir:id [ fhir:v "{fhirID}" ] ;
            {meta}
            {identifier}
            fhir:name   [ fhir:v "{name}" ] ;
            fhir:active [ fhir:v "{str(active).lower()}"^^xsd:boolean ] ;
            fhir:type [
{coding_list}
            ] .

"""
        write_to_middle(insert)
    time_end = time.time()
    move_to_final()
    print(f"organization entity creation took {time_end - time_start:.4f} seconds")

def create_location_entities():
    print("creating location entities")
    time_start = time.time()
    pipeline = [
        {"$match":{"resourceType":"Location"}},
        {"$project":{"_id":1,"id":1,"status":1,"name":1,"physicalType":1,"managingOrganization":1,"meta":1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID=result.get('id')
        status=result.get('status')
        name=result.get('name')
        meta = get_meta(result.get('meta', {}))
        type_list = result.get('physicalType', [])
        coding_list=get_coding(type_list.get('coding', [])) if type_list else []
        managingOrganization=split_refrence(result['managingOrganization']['reference'])
        insert =f"""se:{fhirID} a fhir:Location ;
            {meta}
            fhir:id [ fhir:v "{fhirID}" ] ;
            fhir:status [ fhir:v "{status}" ] ;
            fhir:name [ fhir:v "{name}" ] ;
            fhir:type [
{coding_list}
            ] ;
            fhir:managingOrganization se:{managingOrganization} .

"""
        write_to_middle(insert)
    time_end = time.time()
    move_to_final()
    print(f"location entity creation took {time_end - time_start:.4f} seconds")

def create_patient_entities():
    print("creating patient entities")
    time_start = time.time()
    
    def get_communication(communication):
        if len(communication) == 0:
            return ""
        
        code = communication[0]['language']['coding'][0]['code']
        system = communication[0]['language']['coding'][0]['system']
        com_line = f"""fhir:communication [
                fhir:language [
                    fhir:coding [
                        fhir:system  [ fhir:v "{system}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{code}" ] ;
                    ]
                ]
            ] ;
            """
        return com_line
    
    def get_extension(extension):
        extensionZero = f"""
            fhir:extension [
                fhir:extension [
                    fhir:url  [ fhir:v "{extension[0]['extension'][0]['url']}"^^xsd:anyURI ] ; 
                    fhir:valueCoding [
                        fhir:system  [ fhir:v "{extension[0]['extension'][0]['valueCoding']['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{extension[0]['extension'][0]['valueCoding']['code']}" ] ;
                        fhir:display [ fhir:v "{extension[0]['extension'][0]['valueCoding']['display']}" ]
                    ]
                ] ;
                fhir:extension [
                    fhir:url  [ fhir:v "{extension[0]['extension'][1]['url']}"^^xsd:anyURI ] ;
                    fhir:valueString [ fhir:v "{extension[0]['extension'][1]['valueString']}"^^xsd:string ] 
                ] ;
                fhir:url  [ fhir:v "{extension[0]['url']}"^^xsd:anyURI ] 
            ] ;
"""
        
        valueLine = f";\n\t\t\t\tfhir:valueCode [ fhir:v \"{extension[1]['valueCode']}\" ] ." if len(extension) > 1 and extension[1].get('valueCode') else ""
        
        extensionOne = f"""
            fhir:extension [
                fhir:extension [
                    fhir:url  [ fhir:v "{extension[1]['extension'][0]['url']}"^^xsd:anyURI ] ; 
                    fhir:valueCoding [ 
                        fhir:system  [ fhir:v "{extension[1]['extension'][0]['valueCoding']['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{extension[1]['extension'][0]['valueCoding']['code']}" ] ;
                        fhir:display [ fhir:v "{extension[1]['extension'][0]['valueCoding']['display']}" ]
                    ]
                ] ;
                fhir:extension [
                    fhir:url  [ fhir:v "{extension[1]['extension'][1]['url']}"^^xsd:anyURI ] ;
                    fhir:valueString [ fhir:v "{extension[1]['extension'][1]['valueString']}"^^xsd:string ]
                ] ;
                fhir:url  [ fhir:v "{extension[1]['url']}"^^xsd:anyURI ] {valueLine} 
            ] ;
            fhir:extension [
                fhir:url  [ fhir:v "{extension[2]['url']}"^^xsd:anyURI ] ;
                fhir:valueCode [ fhir:v "{extension[2]['valueCode']}"^^xsd:string ]
            ] ; 
        
""" if extension and 'extension' in extension[1] else ""
        return extensionZero + extensionOne

    pipeline = [
        {"$match": {"resourceType": "Patient"}},
        {"$project": {"_id": 1, "id": 1, "birthDate": 1, "deceasedDateTime": 1, "extension": 1, 
                     "identifier": 1, "gender": 1, "maritalStatus": 1, "communication": 1, 
                     "managingOrganization": 1, "meta": 1}}
    ]
    
    results = collection.aggregate(pipeline)
    
    for result in results:
        meta = get_meta(result.get('meta', {}))
        fhirID = result.get('id')
        identifier = get_identifier(result.get('identifier', []))
        maritalStatusCode = result['maritalStatus']['coding'][0]['code']
        maritalStatusSystem = result['maritalStatus']['coding'][0]['system']
        managingOrganization = split_refrence(result['managingOrganization']['reference'])
        communication = get_communication(result.get('communication', []))
        gender = result.get('gender')
        birthDate = result.get('birthDate')
        deceased = result.get('deceasedDateTime')
        deceasedDateTime = f"fhir:deceasedDateTime [ fhir:v \"{deceased}\"^^xsd:date ] ;" if deceased else ""
        extension = get_extension(result.get('extension', []))
        
        insert = f"""se:{fhirID} a fhir:Patient ;
            fhir:id [ fhir:v "{fhirID}" ] ;
            {meta}
{extension}
            {communication}{identifier}
            {deceasedDateTime}
            fhir:gender [ fhir:v "{gender}" ] ;
            fhir:birthDate [ fhir:v "{birthDate}"^^xsd:date ] ;
            fhir:managingOrganization se:{managingOrganization} ;
            fhir:maritalStatus [
                fhir:coding [
                    fhir:system  [ fhir:v "{maritalStatusSystem}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{maritalStatusCode}" ] 
                ]
            ] .
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"patient entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_encounter_entities():
    print("creating entcounter entities")  
    time_start = time.time()
    def get_identifier_with_reference(identifier):
        identifierSystem = identifier[0].get('system', "") if identifier else ""
        identifierValue = identifier[0].get('value', "") if identifier else ""
        useLine = f";\n\t\t\tfhir:use [ fhir:v \"{identifier[0]['use']}\" ] ;" if identifier[0].get('use') else ""
        assigner = f";\n\t\t\tfhir:assigner [ fhir:v \"{split_refrence(identifier[0]['assigner']['reference'])}\" ]" if identifier[0].get('assigner') else ""
        identifier_line = f"""fhir:identifier [
                fhir:system [ fhir:v "{identifierSystem}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{identifierValue}" ] {useLine}{assigner}
            ] ;"""
        return identifier_line  
    def get_hospitalization(hospital):
        if hospital == "":
            return ""
        
        hos = f"""
            fhir:admitSource [
                fhir:system  [ fhir:v "{hospital['admitSource']['coding'][0]['system']}"^^xsd:anyURI ] ;
                fhir:code    [ fhir:v "{hospital['admitSource']['coding'][0]['code']}" ] 
            ] ;
            
        """ if hospital.get('admitSource') else ""
        
        pital = f"""fhir:dischargeDisposition [
                fhir:system  [ fhir:v "{hospital['dischargeDisposition']['coding'][0]['system']}"^^xsd:anyURI ] ;
                fhir:code    [ fhir:v "{hospital['dischargeDisposition']['coding'][0]['code']}" ] 
            ]""" if hospital.get('dischargeDisposition') else ""
        
        return f"""fhir:hospitalization [
                {hos + pital}
        ] ;
        """  
    def get_serviceType(service):
        if len(service) == 0:
            return ""
        
        service_line = f"""fhir:serviceType [
            fhir:coding [
                fhir:system [ fhir:v "{service['coding'][0]['system']}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{service['coding'][0]['system']}" ] ;
                ]
            ]
            ;"""
        return service_line  
    def get_priority(priority):
        if len(priority) == 0:
            return ""
        
        priority_line = f"""fhir:priority [
            fhir:coding [
                fhir:system [ fhir:v "{priority['coding'][0]['system']}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{priority['coding'][0]['system']}" ] ;
                ]
            ]
            ;"""
        return priority_line 
    def get_period(period):
        period_line = f"""fhir:period [
                fhir:start [ fhir:v "{period['start']}"^^xsd:date ] ;
                fhir:end [ fhir:v "{period['end']}"^^xsd:date ] 
            ] """
        return period_line
    def get_type(typa):
        codings = ""
        for x in typa:
            coding = f"""\t\t\tfhir:type [
{get_coding(x['coding'])}
            ] ;"""
        codings = codings + coding
        return codings  
    def get_location(location):
        locations = ""
        for x in location:
            reference = split_refrence(x['location']['reference'])
            period = get_period(x['period'])
            location_line = f"""fhir:location [
                fhir:location se:{reference} ;
                {period}
            ] ;
            """
            locations = locations + location_line
        return locations
    pipeline = [
        {"$match": {"resourceType": "Encounter"}},
        {"$project": {"_id": 1, "id": 1, "status": 1, "class": 1, "type": 1, "subject": 1, 
                     "location": 1, "partOf": 1, "hospitalization": 1, "identifier": 1,
                     "meta": 1, "priority": 1, "serviceProvider": 1, "serviceType": 1, "period": 1}}
    ]  
    results = collection.aggregate(pipeline)
    for result in results:
        patient = split_refrence(result['subject']['reference'])
        fhirID = result.get('id')
        identifier = get_identifier_with_reference(result.get('identifier', []))
        meta = get_meta(result.get('meta', []))
        hospitalization = get_hospitalization(result.get('hospitalization', ""))
        partOf = f"fhir:partOf se:{split_refrence(result['partOf']['reference'])} ;" if result.get("partOf") else ""
        serviceProvider = f"fhir:serviceProvider se:{split_refrence(result['serviceProvider']['reference'])} ;" if result.get('serviceProvider') else ""
        serviceType = get_serviceType(result.get('serviceType', []))
        priority = get_priority(result.get('priority', []))
        periodLine = get_period(result.get('period'))
        location = get_location(result.get('location', ''))
        type_line = get_type(result.get('type', []))   
        insert = f"""se:{fhirID} a fhir:Encounter ;
            fhir:id [ fhir:v "{fhirID}" ] ;
            fhir:class [
                fhir:system  [ fhir:v "{result['class']['system']}"^^xsd:anyURI ] ;
                fhir:code    [ fhir:v "{result['class']['code']}" ] 
            ] ;
            fhir:status [ fhir:v "{result['status']}" ] ;
            {periodLine} ;
            {partOf}
            {serviceProvider}
{type_line}
            {meta}{serviceType}{priority}
            {location}{identifier}
            {hospitalization}
            fhir:subject se:{patient}  .
        
"""
        write_to_middle(insert)
    move_to_final()
    time_end = time.time()
    print(f"encounter entity creation took {time_end - time_start:.4f} seconds")
    
def create_procedure_entities():
    print("creating procedure entities")
    time_start = time.time()
    def get_category(cat):
        if len(cat) == 0:
            return ""
        
        catCode = cat['coding'][0]['code']
        catSystem = cat['coding'][0]['system']
        return f"""fhir:category [
                fhir:coding [
                    fhir:system  [ fhir:v "{catSystem}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{catCode}" ]
                ]
            ];
    """   
    def get_period(per):
        if len(per) == 0:
            return ""
        
        start = per['start']
        end = per['end']
        return f"""fhir:performedPeriod [
                fhir:start [ fhir:v "{start}"^^xsd:date ] ;
                fhir:end [ fhir:v "{end}"^^xsd:date ] 
                ];
    """   
    def get_body(body):
        if len(body) == 0:
            return ""
        
        catCode = body[0]['coding'][0]['code']
        catSystem = body[0]['coding'][0]['system']
        return f"""fhir:bodySite [
                fhir:system  [ fhir:v "{catSystem}"^^xsd:anyURI ] ;
                fhir:code    [ fhir:v "{catCode}" ]
            ];
    """
    
    def get_code_list(code):
        return get_coding(code['coding'])+";"
    
    pipeline = [
        {"$match": {"resourceType": "Procedure"}},
        {"$project": {"_id": 1, "performedDateTime": 1, "performedPeriod": 1, "id": 1, 
                     "status": 1, "category": 1, "code": 1, "encounter": 1, "bodySite": 1, 
                     "subject": 1, "identifier": 1, "meta": 1}}
    ]
    
    results = collection.aggregate(pipeline)
    
    for result in results:
        fhirID = result['id']
        status = result['status']
        patient_reference = split_refrence(result['subject']['reference'])
        encounter_reference = split_refrence(result['encounter']['reference'])
        identifier = get_identifier(result.get('identifier', []))
        coding_list = get_code_list(result['code'])
        meta = get_meta(result.get('meta', {}))
        category_list = get_category(result.get('category', []))
        performedDateTime = f"fhir:performedDateTime [ fhir:v \"{result.get('performedDateTime')}\"^^xsd:date ] ;" if result.get('performedDateTime') else ""
        performedPeriod = get_period(result.get('performedPeriod', []))
        bodySite = get_body(result.get('bodySite', []))
        
        insert = f"""se:{fhirID} a fhir:Procedure;
            fhir:id   [ fhir:v "{fhirID}" ] ;
            fhir:status [ fhir:v "{status}" ] ;
            fhir:encounter se:{encounter_reference} ;
            {identifier}
            {meta}
            fhir:code [
{coding_list}
            ];
            {category_list}
            {performedDateTime}
            {performedPeriod}
            {bodySite}
            fhir:subject se:{patient_reference} .

"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"procedure entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_condition_entities():
    print("creating condition entities")
    time_start = time.time()
    pipeline = [
        {"$match": {"resourceType": "Condition"}},
        {"$project": {"_id": 1, "id": 1, "identifier": 1, "category": 1, "code": 1, 
                     "subject": 1, "encounter": 1, "meta": 1}}
    ]
    
    results = collection.aggregate(pipeline)
    
    for result in results:
        fhirID = result.get('id')
        identifier = get_identifier(result.get('identifier', []))
        cat = result['category'][0]['coding'][0]
        catCode = cat['code']
        catSystem = cat['system']
        meta = get_meta(result.get('meta', {}))
        code = get_coding(result['code'].get('coding', []))
        patient = split_refrence(result['subject']['reference'])
        encounter = split_refrence(result['encounter']['reference'])
        
        insert = f"""se:{fhirID} a fhir:Condition;
            fhir:id [ fhir:v "{fhirID}" ] ;
            fhir:category [
                fhir:coding [
                    fhir:system  [ fhir:v "{catSystem}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{catCode}" ] 
                ]
            ] ;
            {meta}
            fhir:code [
{code}
            ] ;
            {identifier}
            fhir:encounter se:{encounter} ;
            fhir:subject se:{patient}. 
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"condition entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medicationDispense_entities():
    print("creating medication dispense entities")
    time_start = time.time()
    def get_dosage(dosage):
        if len(dosage) == 0:
            return ""
        
        def get_repeat(repeat):
            if len(repeat) == 0:
                return ""
            
            return f""";\n\t\t\t\t\tfhir:repeat [
                        fhir:duration [ fhir:v "{repeat['duration']}"^^xsd:decimal ] ; 
                        fhir:durationUnit [ fhir:v "{repeat['durationUnit']}" ]
                    ]
        """
        
        route_coding = get_small_coding(dosage[0]['route']['coding'])
        route = f"""fhir:route [
                    {route_coding}
                ]"""
        
        timing = f""";\n\t\t\t\tfhir:timing [
                       {get_small_coding(dosage[0]['timing']['code']['coding'])}{get_repeat(dosage[0]['timing'].get('repeat', []))}
               ]""" if len(dosage[0].get('timing', [])) > 0 else ""
        
        maxDose = f""";\n\t\t\t\tfhir:maxDosePerPeriod [
                    fhir:denominator [
                        fhir:system [ fhir:v "{dosage[0]['maxDosePerPeriod']['denominator']['system']}"^^xsd:anyURI ] ;
                        fhir:unit   [ fhir:v "{dosage[0]['maxDosePerPeriod']['denominator']['unit']}" ] ;
                        fhir:value  [ fhir:v "{dosage[0]['maxDosePerPeriod']['denominator']['value']}"^^xsd:decimal ]
                    ] ;
                    fhir:numerator [
                        fhir:value [ fhir:v "{dosage[0]['maxDosePerPeriod']['numerator']['value']}"^^xsd:decimal ]
                    ] 
                ]
        """ if len(dosage[0].get('maxDosePerPeriod', [])) > 0 else ""
        
        return route + timing + maxDose
    
    pipeline = [
        {"$match": {"resourceType": "MedicationDispense"}},
        {"$project": {"_id": 1, "id": 1, "identifier": 1, "context": 1, "authorizingPrescription": 1, 
                     "medicationCodeableConcept": 1, "subject": 1, "dosageInstruction": 1, "meta": 1, "status": 1}}
    ]
    
    results = collection.aggregate(pipeline)
    
    for result in results:
        fhirID = result['id']
        identifier = get_identifier(result.get('identifier'))
        context = split_refrence(result['context']['reference'])
        subject = split_refrence(result['subject']['reference'])
        authorizingPrescription = split_refrence(result['authorizingPrescription'][0]['reference'])
        meta = get_meta(result.get('meta', {}))
        mccCoding = get_small_coding(result['medicationCodeableConcept']['coding'])
        status = result['status']
        dosage = get_dosage(result.get('dosageInstruction', []))
        
        insert = f"""se:{fhirID} a fhir:MedicationDispense;
            fhir:id [ fhir:v "{fhirID}" ] ;
            {identifier}
            fhir:context se:{context} ;
            fhir:subject se:{subject} ;
            fhir:authorizingPrescription se:{authorizingPrescription} ;
            {meta}
            fhir:medicationCodeableConcept [
                {mccCoding}
            ] ;
            fhir:status [ fhir:v "{status}" ] ;
            fhir:dosageInstruction [
                {dosage}
            ] .
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"medication dispense entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medicationRequest_entities():
    def get_dispense_request(dispense):
        if len(dispense) == 0:
            return ""
        return f"""fhir:dispenseRequest [
                fhir:validityPeriod [
                    fhir:start [ fhir:v "{dispense['validityPeriod']['start']}"^^xsd:date ] ;
                    fhir:end [ fhir:v "{dispense['validityPeriod']['end']}"^^xsd:date ]
                ]
            ] ;
"""

    def get_dosage_instruction(dosage):
        if len(dosage) == 0:
            return ""
        
        def get_repeat(repeat):
            if len(repeat) == 0:
                return ""
            return f""";\n\t\t\t\t\tfhir:repeat [
                        fhir:duration [ fhir:v "{repeat['duration']}"^^xsd:decimal ] ; 
                        fhir:durationUnit [ fhir:v "{repeat['durationUnit']}" ]
                    ]
        """
        
        route_coding = get_small_coding(dosage[0]['route']['coding'])
        text_line = f";\n\t\t\t\tfhir:text [ fhir:v \"{dosage[0]['text']}\"]" if dosage[0].get('text') else ""
        route = f"""\t\tfhir:route [
                    fhir:coding [
                        fhir:system  [ fhir:v "{dosage[0]['route']['coding'][0]['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{dosage[0]['route']['coding'][0]['code']}" ]
                    ]
                ] {text_line}"""
        
        timing = f"""fhir:timing [
                    fhir:coding [
                        fhir:system  [ fhir:v "{dosage[0]['timing']['code']['coding'][0]['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{dosage[0]['timing']['code']['coding'][0]['code']}" ]
                    ]{get_repeat(dosage[0]['timing'].get('repeat', []))}
                ] ;
""" if len(dosage[0].get('timing', [])) > 0 else ""
        
        maxDose = f"""fhir:maxDosePerPeriod [
                    fhir:denominator [
                        fhir:system [ fhir:v "{dosage[0]['maxDosePerPeriod']['denominator']['system']}"^^xsd:anyURI ] ;
                        fhir:unit   [ fhir:v "{dosage[0]['maxDosePerPeriod']['denominator']['unit']}" ] ;
                        fhir:value  [ fhir:v "{dosage[0]['maxDosePerPeriod']['denominator']['value']}"^^xsd:decimal ]
                    ] ;
                    fhir:numerator [
                        fhir:value [ fhir:v "{dosage[0]['maxDosePerPeriod']['numerator']['value']}"^^xsd:decimal ]
                    ] 
                ] ;
""" if len(dosage[0].get('maxDosePerPeriod', [])) > 0 else ""
        
        doseAndRate = f"""\t\t\t\tfhir:doseAndRate [
                    fhir:doseQuantity [
                        fhir:system [ fhir:v "{dosage[0]['doseAndRate'][0]['doseQuantity']['system']}"^^xsd:anyURI ] ;
                        fhir:unit   [ fhir:v "{dosage[0]['doseAndRate'][0]['doseQuantity']['unit']}" ] ;
                        fhir:value  [ fhir:v "{dosage[0]['doseAndRate'][0]['doseQuantity']['value']}"^^xsd:decimal ] ;
                        fhir:code    [ fhir:v "{dosage[0]['doseAndRate'][0]['doseQuantity']['code']}" ]
                    ]
                ] ; 
        """ if len(dosage[0].get('doseAndRate', [])) > 0 else ""

        return timing + maxDose + doseAndRate + route

    def get_mr_identifier(identifier):
        return f"""fhir:identifier [
                fhir:system [ fhir:v "{identifier[0]['system']}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{identifier[0]['value']}" ] ;
                fhir:type [
                    fhir:coding [
                        fhir:system  [ fhir:v "{identifier[0]['type']['coding'][0]['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{identifier[0]['type']['coding'][0]['code']}" ] ;
                        fhir:display [ fhir:v "{identifier[0]['type']['coding'][0]['display']}" ]
                    ]
                ]
            ] ;

"""

    def get_mcc(mcc):
        if len(mcc) == 0:
            return ""
        return f"""fhir:medicationCodeableConcept [
            fhir:coding [
                fhir:system  [ fhir:v "{mcc['coding'][0]['system']}"^^xsd:anyURI ] ;
                fhir:code    [ fhir:v "{mcc['coding'][0]['code']}" ] 
            ]
        ] ;
"""

    time_start = time.time()
    print("creating medication request entities")
    pipeline = [
        {"$match": {"resourceType": "MedicationRequest"}},
        {"$project": {"identifier": 1, "authoredOn": 1, "dispenseRequest": 1, "dosageInstruction": 1, "id": 1, "encounter": 1, "intent": 1, "type": 1,
                     "medicationCodeableConcept": 1, "meta": 1, "status": 1, "subject": 1, "medicationReference": 1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID = result.get('id')
        meta = get_meta(result.get('meta', {}))
        authoredOn = result['authoredOn']
        subject = split_refrence(result['subject']['reference'])
        encounter = split_refrence(result['encounter']['reference'])
        dispenseRequest = get_dispense_request(result.get('dispenseRequest', []))
        dosageInstruction = get_dosage_instruction(result.get('dosageInstruction', []))
        identifier = get_mr_identifier(result['identifier'])
        mcc = get_mcc(result.get('medicationCodeableConcept', []))
        intent = result['intent']
        status = result['status']
        medication = f"fhir:medicationReference se:{split_refrence(result['medicationReference']['reference'])} ;" if len(result.get('medicationReference', [])) > 0 else ""
        insert = f"""se:{fhirID} a fhir:MedicationRequest ;
            fhir:id [ fhir:v "{fhirID}" ] ;
            fhir:encounter se:{encounter}  ;
            fhir:subject se:{subject}  ;
            fhir:intent [ fhir:v "{intent}" ] ;
            fhir:status [ fhir:v "{status}"] ;
            {medication}
            {meta} 
            {dispenseRequest}
            fhir:dosageInstruction [
                {dosageInstruction}
            ] ;
            {identifier}
            {mcc}
            fhir:authoredOn [ fhir:v "{authoredOn}"^^xsd:date ] .
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"medication request entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_specimen_entities():
    def get_specimen_type(typa):
        display=f";\n\t\t\t\t\tfhir:display [ fhir:v \"{typa['coding'][0]['display']}\" ]" if typa['coding'][0].get('display') else ""
        return f"""fhir:type [
                fhir:coding[
                    fhir:system  [ fhir:v "{typa['coding'][0]['system']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{typa['coding'][0]['code']}" ] {display}
                ]
            ] ;
            """
    def get_collection(col):
        if len(col)==0:
            return ""
        return f"""fhir:collection [
                fhir:collectedDateTime [ fhir:v "{col['collectedDateTime']}"^^xsd:date ] 
            ] ; 
            """
        pass
    time_start = time.time()
    print("creating specimen entities")
    pipeline = [
        {"$match":{"resourceType":"Specimen"}},
        {"$project":{"_id":1,"id":1,"identifier":1,"collection":1,"type":1,"subject":1,"meta":1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID=result.get('id')
        identifier = get_identifier(result.get('identifier', []))
        meta = get_meta(result.get('meta', {}))
        type_list = get_specimen_type(result.get('type', []))
        subject = split_refrence(result['subject']['reference'])
        collectedDateTime = f"\t\t\tfhir:collectedDateTime [ fhir:v \"{result['collection']['collectedDateTime']}\"^^xsd:date ] ;" if len(result.get('collection',[]))!=0 else ""
        insert =f"""se:{fhirID} a fhir:Specimen ;
            fhir:id [ fhir:v "{fhirID}" ] ;
            {meta}
            {identifier}
            {type_list}
{collectedDateTime}
            fhir:subject se:{subject} .
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"specimen entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medication_entities():
    def get_medication_identifier(identifier):
        identifiers=""
        for x in identifier:
            identifiers=identifiers+f"""\t\t\tfhir:identifier [
                fhir:system [ fhir:v "{x['system']}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{escape_turtle_string(x['value'])}" ]
            ] ;
"""
        return identifiers
    def get_ingredients(ing):
        if len(ing)==0:
            return ""
        ingredients=""
        for x in ing:
            ingredients=ingredients + f"\t\t\tfhir:ingredient se:{split_refrence(x['itemReference']['reference'])} ;\n"
        return ingredients
    def get_medication_code(coda):
        if len(coda)==0:
            return ""
        return f"""fhir:code [
                fhir:coding [
                    fhir:system  [ fhir:v "{coda['coding'][0]['code']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{coda['coding'][0]['code']}" ]
                ] 
            ];
"""
    time_start = time.time()
    print("creating Medication entities")
    pipeline = [
        {"$match":{"resourceType":"Medication"}},
        {"$project":{"_id":1,"id":1,"identifier":1,"ingredient":1,"code":1,"meta":1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID=result.get('id')
        identifier=get_medication_identifier(result['identifier'])
        code=get_medication_code(result.get('code',[]))
        meta = get_meta(result.get('meta', {}))
        ingredients=get_ingredients(result.get('ingredient',[]))
        insert =f"""se:{fhirID} a fhir:Medication ;
            {meta}
{identifier}
{ingredients}
            {code}
            fhir:id [ fhir:v "{fhirID}" ] .

"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"medication entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medicationAdministration_entities():
    def get_category(cat):
        if len(cat)==0:
            return ""
        return f"""\t\t\tfhir:category [
                fhir:coding [
                    fhir:system  [ fhir:v "{cat['coding'][0]['system']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{cat['coding'][0]['code']}" ]
                ] 
            ];
"""
    def get_dosage(dosage):
        if len(dosage)==0:
            return ""
        text = f"\t\t\t\tfhir:text [ fhir:v \"{dosage['text']}\" ] ;\n" if dosage.get('text',None) else ""
        rateQuantity = f"""\t\t\t\tfhir:rateQuantity [
                    fhir:system [ fhir:v "{dosage['rateQuantity']['system']}"^^xsd:anyURI ] ;
                    fhir:unit   [ fhir:v "{dosage['rateQuantity']['unit']}" ] ;
                    fhir:value  [ fhir:v "{dosage['rateQuantity']['value']}"^^xsd:decimal ] ;
                    fhir:code    [ fhir:v "{dosage['rateQuantity']['code']}" ] 
                ] ;
""" if dosage.get('rateQuantity',None) else ""
        method = f"""\t\t\t\tfhir:method [
                    fhir:coding [
                        fhir:system [ fhir:v "{dosage['method']['coding'][0]['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{dosage['method']['coding'][0]['code']}" ] 
                    ]
                ] ;
""" if dosage.get('method',None) else ""
        dose_code=f" ;\n\t\t\t\t\tfhir:code    [ fhir:v \"{escape_turtle_string(dosage['dose']['code'])}\" ]" if dosage['dose'].get('code') else ""
        dose_unit=f" ;\n\t\t\t\t\tfhir:unit   [ fhir:v \"{escape_turtle_string(dosage['dose']['unit'])}\" ]" if dosage['dose'].get('unit') else ""
        dose = f"""\t\t\t\tfhir:dose [
                    fhir:system [ fhir:v "{dosage['dose']['system']}"^^xsd:anyURI ] ;
                    fhir:value  [ fhir:v "{dosage['dose']['value']}"^^xsd:decimal ]{dose_code}{dose_unit}
                ]
"""
        return f"""\t\t\tfhir:dosage [
{rateQuantity}{text}{method}{dose}
            ] ;
"""
    def get_period(period):
        if len(period)==0:
            return ""
        period_line = f"""\t\t\tfhir:effectivePeriod [
                fhir:start [ fhir:v "{period['start']}"^^xsd:date ] ;
                fhir:end [ fhir:v "{period['end']}"^^xsd:date ] 
            ] ;
"""
        return period_line
    def get_medication_code(coda):
        if len(coda)==0:
            return ""
        display=f" ;\n\t\t\t\t\tfhir:display [ fhir:v \"{coda['coding'][0]['display']}\" ]" if coda['coding'][0].get('display',None) else ""
        return f"""\t\t\tfhir:code [
                fhir:coding [
                    fhir:system  [ fhir:v "{coda['coding'][0]['code']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{coda['coding'][0]['code']}" ]{display}
                ]
            ] ;
"""
    def get_ma_identifier(iden):
        return f"""\t\t\tfhir:identifier [
                fhir:system  [ fhir:v "{iden['system']}"^^xsd:anyURI ] ;
                fhir:value  [ fhir:v "{iden['value']}" ];
                fhir:type [
                    fhir:coding [
                        fhir:system  [ fhir:v "{iden['type']['coding'][0]['system']}"^^xsd:anyURI ] ;
                        fhir:code    [ fhir:v "{iden['type']['coding'][0]['code']}" ] ;
                        fhir:display [ fhir:v "{iden['type']['coding'][0]['display']}" ]
                    ]
                ]
            ] ;
"""
    time_start = time.time()
    print("creating medication administration entities")
    pipeline = [
        {"$match":{"resourceType":"MedicationAdministration"}},
        {"$project":{"id":1,"meta":1,"category":1,"context":1,"dosage":1,"effectiveDateTime":1,"identifier":1,
                     "medicationCodeableConcept":1,"request":1,"status":1,"subject":1, "effectivePeriod":1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID=result.get('id')
        effectiveDateTime =f"\t\t\tfhir:effectiveDateTime [ fhir:v \"{result['effectiveDateTime']}\"^^xsd:date ] ;" if len(result.get('effectiveDateTime',[])) else ""
        request =f"\t\t\tfhir:request se:{split_refrence(result['request']['reference'])} ;" if len(result.get('request',[]))!=0 else ""
        context =f"\t\t\tfhir:context se:{split_refrence(result['context']['reference'])} ;" if len(result.get('context',[]))!=0 else ""
        meta = get_meta(result.get('meta', {}))
        category=get_category(result.get('category',[]))
        dosage=get_dosage(result.get('dosage',[]))
        period=get_period(result.get('effectivePeriod',[]))
        mcc=get_medication_code(result['medicationCodeableConcept'])
        status=result['status']
        subject=split_refrence(result['subject']['reference'])
        identifer=get_ma_identifier(result['identifier'][0])
        insert =f"""se:{fhirID} a fhir:MedicationAdministration ;
            fhir:id [ fhir:v "{fhirID}" ] ;
{request}
{mcc}
{identifer}
{category}
{context}
{period}
{effectiveDateTime}
{dosage}
            {meta}
            fhir:subject se:{subject} ; 
            fhir:status [ fhir:v "{status}"] .

"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"medication administration entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_observation_entities():
    def get_category(cat):
        return f"""\t\t\tfhir:category [
                fhir:coding [
                    fhir:system  [ fhir:v "{cat[0]['coding'][0]['system']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{cat[0]['coding'][0]['code']}" ] 
                ]
            ] .
"""
    def get_o_code(code):
        return f"""\t\t\tfhir:coding [
                fhir:system  [ fhir:v "{code['coding'][0]['system']}"^^xsd:anyURI ] ;
                fhir:code    [ fhir:v "{code['coding'][0]['system']}" ] ;
                fhir:display [ fhir:v "{code['coding'][0]['display']}" ]
            ];
"""
    def get_extension(ex):
        if len(ex)==0:
            return ""
        comparator=f"\t\t\t\t\tfhir:comparator [ fhir:v \"{ex[0]['valueQuantity']['comparator']}\" ] ;" if len(ex[0].get('valueQuantity',[]))!=0 and len(ex[0]['valueQuantity'].get('comparator',[]))!=0 else ""
        valueQuantity=f"""\t\t\t\tfhir:valueQuantity [
{comparator}
                    fhir:value [ fhir:v "{ex[0]['valueQuantity']['value']}" ] 
                ] ;
""" if len(ex[0].get('valueQuantity',[]))!=0 else ""
        valueString=f"\t\t\t\tfhir:valueString [ fhir:v \"{ex[0]['valueString']}\" ] ;" if len(ex[0].get('valueString',[]))!=0 else ""
        return f"""\t\t\tfhir:extension [
{valueString}
{valueQuantity}
                fhir:url  [ fhir:v "{ex[0]['url']}"^^xsd:anyURI ]
            ] ;
    """
    def get_members(members):
        if len(members)==0:
            return ""
        references=""
        for x in members:
            references=references+f"\t\t\tfhir:hasMember se:{split_refrence(x['reference'])} ;\n"
        return references
    def get_interpretation(inter):
        if len(inter)==0:
            return ""
        return f"""fhir:interpretation [
                fhir:coding [
                    fhir:system  [ fhir:v "{inter[0]['coding'][0]['system']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{inter[0]['coding'][0]['code']}" ] 
                ]
            ] ;
"""
    def get_note(note):
        if len(note)==0:
            return ""
        return f"""\t\t\tfhir:note [
                fhir:text [ fhir:v "{sanitize_for_kg_literal(note[0]['text'])}" ]
            ] ;
"""
    def get_referenceRange(rr):
        if len(rr)==0:
            return ""
        high_unit=f";\n\t\t\t\t\tfhir:unit   [ fhir:v \"{rr[0]['high']['unit']}\" ]" if len(rr[0].get('high',[]))!=0 and rr[0]['high'].get('unit',None) else ""
        high_value=f";\n\t\t\t\t\tfhir:value   [ fhir:v \"{rr[0]['high']['value']}\" ]" if len(rr[0].get('high',[]))!=0 and rr[0]['high'].get('value',None) else ""
        high_code=f";\n\t\t\t\t\tfhir:code   [ fhir:v \"{rr[0]['high']['code']}\" ]" if len(rr[0].get('high',[]))!=0 and rr[0]['high'].get('code',None) else ""
        high=f"""\t\t\t\tfhir:high [
                    fhir:system [ fhir:v \"{rr[0]['high']['system']}\"^^xsd:anyURI ]{high_unit}{high_value}{high_code}
                ]""" if len(rr[0].get('high',[]))!=0 else ""
        
        low_unit=f";\n\t\t\t\t\tfhir:unit   [ fhir:v \"{rr[0]['low']['unit']}\" ]" if len(rr[0].get('low',[]))!=0 and rr[0]['low'].get('unit',None) else ""
        low_value=f";\n\t\t\t\t\tfhir:value   [ fhir:v \"{rr[0]['low']['value']}\" ]" if len(rr[0].get('low',[]))!=0 and rr[0]['low'].get('value',None) else ""
        low_code=f";\n\t\t\t\t\tfhir:code   [ fhir:v \"{rr[0]['low']['code']}\" ]" if len(rr[0].get('low',[]))!=0 and rr[0]['low'].get('code',None) else ""
        low=f"""\t\t\t\tfhir:low [
                    fhir:system [ fhir:v \"{rr[0]['low']['system']}\"^^xsd:anyURI ]{low_unit}{low_value}{low_code}
                ]
""" if len(rr[0].get('low',[]))!=0 else ""
        space=""
        if low!=""and high!="":
            space=";"
        return f"""\t\t\tfhir:referenceRange [
{high}{space}
{low}
            ] ;
"""
    def get_vcc(vcc):
        if len(vcc)==0:
            return ""
        display=f";\n\t\t\t\t\tfhir:display [ fhir:v \"{vcc['coding'][0]['display']}\" ]" if vcc['coding'][0].get('display',None) else ""
        return f"""\t\t\tfhir:valueCodeableConcept [
                fhir:coding [
                    fhir:system  [ fhir:v "{vcc['coding'][0]['system']}"^^xsd:anyURI ] ;
                    fhir:code    [ fhir:v "{vcc['coding'][0]['code']}" ]
                ]
            ] ;
"""
    def get_valueQuantity(vq):
        if len(vq)==0:
            return ""
        value =f";\n\t\t\t\tfhir:value [ fhir:v \"{vq['value']}\"^^xsd:decimal ]" if vq.get('value',None) else ""
        comparator =f";\n\t\t\t\tfhir:comparator [ fhir:v \"{vq['comparator']}\" ]" if vq.get('comparator',None) else ""
        code = f";\n\t\t\t\tfhir:code [ fhir:v \"{vq['code']}\" ]" if vq.get('code',None) else ""
        return f"""\t\t\tfhir:valueQuantity [
                fhir:system [ fhir:v "{vq['system']}"^^xsd:anyURI ]{value}{comparator}{code}
            ] ;
"""
    time_start = time.time()
    print("creating observation entities")
    pipeline = [
        {"$match":{"resourceType":"Observation","dataAbsentRearson":{"$exists": False}}},
        {"$project":{"id":1,"category":1,"code":1,"derivedFrom":1,"effectiveDateTime":1,"encounter":1,"extension":1,"specimen":1,
                     "status":1,"subject":1,"identifier":1,"meta":1,"hasMember":1,"interpretation":1,"issued":1,"valueDateTime":1,
                     "valueString":1,"note":1,"referenceRange":1,"valueCodeableConcept":1,"valueQuantity":1}}
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        fhirID=result.get('id')
        category=get_category(result['category'])
        code=get_o_code(result['code'])
        derivedFrom =f"\t\t\tfhir:derivedFrom se:{split_refrence(result['derivedFrom'][0]['reference'])} ;" if len(result.get('derivedFrom',[]))!=0 else ""
        encounter =f"\t\t\tfhir:encounter se:{split_refrence(result['encounter']['reference'])} ;" if len(result.get('encounter',[]))!=0 else ""
        specimen =f"\t\t\tfhir:specimen se:{split_refrence(result['specimen']['reference'])} ;" if len(result.get('specimen',[]))!=0 else ""
        status = f"\t\t\tfhir:status [ fhir:v \"{result['status']}\" ] ;"
        subject = f"\t\t\tfhir:subject se:{split_refrence(result['subject']['reference'])} ;"
        effectiveDateTime=f"\t\t\tfhir:effectiveDateTime [ fhir:v \"{result['effectiveDateTime']}\"^^xsd:dateTime ] ;" if result.get('effectiveDateTime',None) else ""
        extension=get_extension(result.get('extension',[]))
        identifier=get_identifier(result['identifier'])
        meta = get_meta(result.get('meta', {}))
        hasMember=get_members(result.get('hasMember',[]))
        interpretation=get_interpretation(result.get('interpretation',[]))
        issued=f"\t\t\tfhir:issued [ fhir:v \"{result['issued']}\"^^xsd:date ] ; " if result.get('issued', None) else ""
        valueDateTime=f"\t\t\tfhir:valueDateTime [ fhir:v \"{result['valueDateTime']}\"^^xsd:dateTime ] ; " if result.get('valueDateTime', None) else ""
        valueString=f"\t\t\tfhir:valueString [ fhir:v \"{sanatize_quotes(result['valueString'])}\" ] ; " if result.get('valueString', None) else ""
        note=get_note(result.get('note',[]))
        referenceRange=get_referenceRange(result.get('referenceRange',[]))
        vcc=get_vcc(result.get('valueCodeableConcept',[]))
        valueQuantity=get_valueQuantity(result.get('valueQuantity',[]))
        insert =f"""se:{fhirID} a fhir:Observation ;
            fhir:id [ fhir:v "{fhirID}" ] ;
{specimen}
{valueString}
{valueDateTime}
{issued}
{valueQuantity}
{status}
{subject}
{note}
{vcc}
{referenceRange}
{interpretation}
{code}
            {meta}
            {identifier}
{hasMember}
{derivedFrom}
{encounter}
{extension}
{effectiveDateTime}
{category}
        
"""
        write_to_middle(insert)
    time_end = time.time()
    move_to_final()
    print(f"observation entity creation took {time_end - time_start:.4f} seconds")

create_ttl_script()