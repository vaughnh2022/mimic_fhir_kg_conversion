#--------------------------------------------------------------
#
# This python file coverts JSON Mimic Fhir Data into a flattened knowledge graph and complete ontology
#
# To create the ttl script you need the updated_mimic_fhir_script.ttl
#
# for full functionality you need the general_queries.py file
#
# I query a local mongoDB database with the MIMIC IV FHIR Demo on it, to use my code you will need to upload a combined ndjson file into your local database 
#
#----------------------------------------------------------------

from pymongo import MongoClient
import json 
import time
import uuid
from fhir_kg_creation import get_distinct_fields, get_fields_in_all_documents, get_resource_type_list, get_sample_from_resource_type, get_unique_values_by_field

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Access a database
db = client["mimic"]
collection = db["fhir"]


#comment out any entity functions you do not want in the final script for test purposes
#it takes roughly a minute to create the full script
def create_ttl_script():
    """
    This calls all needed functions to create the full knowledge graph and output it to final_script.ttl
    """
    time_start = time.time()
    with open("flattened_final_script.ttl", "w") as f:
        f.write("")
    print("writing to file")
    with open('flattened_kg_script.ttl', 'r', encoding='utf-8') as file:
        ttl_string = file.read()
    with open("flattened_final_script.ttl", "w") as f:
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
    with open("flattened_final_script.ttl", "r", encoding="utf-8") as f:
        for line in f:
            line_count += 1
            char_count += len(line)
    time_end = time.time()
    print(f"Script completed in {time_end - time_start:.4f} seconds")   
    print(f"Character count: {char_count}")
    print(f"Line count: {line_count}")


#database specific functions
#these output to the terminal

def collect_one_pipeline():
    """
    This fuction prints a document from a collect one pipeline. Use this to define specific fields you want to exist and see the json output.
    """
    result = collection.find_one({"resourceType":"Encounter","serviceType":{"$exists":True}})

    print(json.dumps(result, indent=2, default=str))


#file writing and sanatization functions

def generate_id(unique_string):
    """
    This fuction creates a id in the structure of the mimic fhir id given a string
    (Note that the same unique_string will output the same id)
    Args:
        unique_string (string): a unique string that is converted to an ID
    Returns:
        str: random id
    """
    namespace = uuid.UUID('ee172322-118b-5716-abbc-18e4c5437e15')
    return uuid.uuid5(namespace, unique_string)

def write_to_middle(insertion):
    """
    appends string to middle_man.txt
    Args:
        insertion (string): a string of text
    """
    with open("middle_man.txt", "a") as f:
            f.write(insertion)

def clear_middle():
    """
    This fuction empties the text file middle_man.txt
    """
    with open("middle_man.txt", "w") as f:
            f.write("")

def move_to_final():
    """
    This function appends middle_man.txt to final_script.ttl then clears middle_man.txt
    """
    with open("middle_man.txt", "r", encoding="utf-8") as txt_file:
        content = txt_file.read()

    with open("flattened_final_script.ttl", "a", encoding="utf-8") as ttl_file:
        ttl_file.write("\n")
        ttl_file.write(content)
    clear_middle()

def split_refrence(refrence):
    """
    This fuction removes the leading *resource_type*/*id* from refrences to construct the knowledge graph connection
    Args:
        refrence (string): a json refrence holding *resource_type*/*id*
    Returns:
        str: just *id*
    """
    return refrence.split("/")[-1]

def escape_turtle_string(s):
    """
    This fuction sanatizes text with slashes so that it compiles under python and rdf rules
    Returns:
        str: sanatized text
    """
    return s.replace('\\', '\\\\').replace('"', '\\"')

def sanatize_quotes(s):
    """
    This fuction sanatizes quotes in a string for python and rdf compliance
    Args:
        s (string): a string of text
    Returns:
        str: sanatized string
    """
    return s.replace('"', '\\"')
    
def sanitize_for_kg_literal(text):
    """
    This fuction sanatizes text for both slashes and quotes
    Args:
        text (string): a string of text
    Returns:
        str: the sanatized text 
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

#these functions create the entities, note this is the general structure below
# create_*resource_type*_entities():
#     
#     collection of functions to convert specific nested fields
#
#     mongoDB pipeline to query the database
#
#     for result in results:
#         iterates through each document
#
#         creates a string of a single entity in the knowledge graph
#
#         appends this single entity to middle_man.txt
#
#     appends middle_man.txt to final_script.ttl then clears middle_man.txt



def create_organization_entities():
    def get_identifier(identifier):
        """
        This fuction converts identifier information into knowledge graph format
        Args:
            identifier (dict): a dictionary holding coding information
        Returns:
            str: the converted identifier property 
        """
        if len(identifier)==0:
            return ""
        identifierSystem=identifier[0].get('system',"")  if identifier else ""
        identifierValue=identifier[0].get('value',"")  if identifier else ""
        identifier_line=f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{sanatize_quotes(identifierValue)}" ;"""
        return identifier_line
    def get_coding(coding):
        """
        This fuction converts coding information into knowledge graph format
        Args:
            coding (dict): a dictionary holding coding information
        Returns:
            str: the converted coding property 
        """
        if len(coding)==0:
            return ""
        typeCodingSystem = coding[0].get('system', '')
        typeCodingCode = coding[0].get('code', '')
        typeCodingDisplay = coding[0].get('display', '')
        coding_line =f"""\t\t\tfhir:typeCodingSystem  "{typeCodingSystem}" ;
            fhir:typeCodingCode "{typeCodingCode}" ;
            fhir:typeCodingDisplay "{typeCodingDisplay}" ;"""
        return coding_line
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
        type_list = result.get('type', [])
        coding_list=get_coding(type_list[0].get('coding', [])) if type_list else []
        insert =f"""se:{fhirID} a fhir:Organization ;
            fhir:id "{fhirID}" ;
{identifier}
            fhir:active "{str(active).lower()}" ;
{coding_list}
            fhir:name "{name}" .

"""
        write_to_middle(insert)
    time_end = time.time()
    move_to_final()
    print(f"organization entity creation took {time_end - time_start:.4f} seconds")

def create_location_entities():
    def get_coding(coding):
        """
        This fuction converts coding information into knowledge graph format
        Args:
            coding (dict): a dictionary holding coding information
        Returns:
            str: the converted coding property 
        """
        if len(coding)==0:
            return ""
        typeCodingSystem = coding[0].get('system', '')
        typeCodingCode = coding[0].get('code', '')
        typeCodingDisplay = coding[0].get('display', '')
        coding_line =f"""\t\t\tfhir:typeCodingSystem  "{typeCodingSystem}" ;
            fhir:typeCodingCode "{typeCodingCode}" ;
            fhir:typeCodingDisplay "{typeCodingDisplay}" ;"""
        return coding_line
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
        type_list = result.get('physicalType', [])
        coding_list=get_coding(type_list.get('coding', [])) if type_list else []
        managingOrganization=split_refrence(result['managingOrganization']['reference'])
        insert =f"""se:{fhirID} a fhir:Location ;
            fhir:id "{fhirID}" ;
            fhir:status "{status}" ;
            fhir:name "{name}" ;
{coding_list}
            fhir:managingOrganizationReference se:{managingOrganization} .

"""
        write_to_middle(insert)
    time_end = time.time()
    move_to_final()
    print(f"location entity creation took {time_end - time_start:.4f} seconds")

def create_patient_entities():
    print("creating patient entities")
    time_start = time.time()

    def get_identifier(identifier):
        """
        This fuction converts identifier information into knowledge graph format
        Args:
            identifier (dict): a dictionary holding coding information
        Returns:
            str: the converted identifier property 
        """
        if len(identifier)==0:
            return ""
        identifierSystem=identifier[0].get('system',"")  if identifier else ""
        identifierValue=identifier[0].get('value',"")  if identifier else ""
        identifier_line=f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{sanatize_quotes(identifierValue)}" ;"""
        return identifier_line

    def get_communication(communication):
        if len(communication) == 0:
            return ""
        code = communication[0]['language']['coding'][0]['code']
        system = communication[0]['language']['coding'][0]['system']
        com_line = f"""\t\t\tfhir:communicationLangaugeCodingCode "{code}" ;
            fhir:communicationLangaugeCodingSystem "{system}" ;"""
        return com_line
    
    def get_extension(extension):
        ethinicity= f"\n\t\t\tfhir:ethnicity \"{extension[1]['extension'][0]['valueCoding']['display']}\" ;" if extension and 'extension' in extension[1] else ""
        extensionZero = f"""\t\t\tfhir:race "{extension[0]['extension'][0]['valueCoding']['display']}" ;{ethinicity}"""
        return extensionZero

    pipeline = [
        {"$match": {"resourceType": "Patient"}},
        {"$project": {"_id": 1, "id": 1, "birthDate": 1, "deceasedDateTime": 1, "extension": 1, 
                     "identifier": 1, "gender": 1, "maritalStatus": 1, "communication": 1, 
                     "managingOrganization": 1, "meta": 1}}
    ]
    
    results = collection.aggregate(pipeline)
    
    for result in results:
        fhirID = result.get('id')
        identifier = get_identifier(result.get('identifier', []))
        maritalStatusCode = result['maritalStatus']['coding'][0]['code']
        maritalStatusSystem = result['maritalStatus']['coding'][0]['system']
        managingOrganization = split_refrence(result['managingOrganization']['reference'])
        communication = get_communication(result.get('communication', []))
        gender = result.get('gender')
        birthDate = result.get('birthDate')
        deceased = result.get('deceasedDateTime')
        deceasedDateTime = f"\n\t\t\tfhir:deceasedDateTime \"{deceased}\" ;" if deceased else ""
        extension = get_extension(result.get('extension', []))
        
        insert = f"""se:{fhirID} a fhir:Patient ;
            fhir:id "{fhirID}" ;
{extension}
{communication}
{identifier}{deceasedDateTime}
            fhir:gender "{gender}" ;
            fhir:birthDate "{birthDate}" ;
            fhir:managingOrganizationReference se:{managingOrganization} ;
            fhir:system "{maritalStatusSystem}" ;
            fhir:code "{maritalStatusCode}" .
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"patient entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_encounter_entities():
    print("creating entcounter entities")  
    time_start = time.time()
    def get_coding(codinga):
        """
        This fuction converts coding information into knowledge graph format
        Args:
            coding (dict): a dictionary holding coding information
        Returns:
            str: the converted coding property 
        """
        if len(codinga)==0:
            return ""
        coding=codinga[0]['coding']
        typeCodingSystem = coding[0].get('system', '')
        typeCodingCode = coding[0].get('code', '')
        typeCodingDisplay = coding[0].get('display', '')
        coding_line =f"""\t\t\tfhir:typeCodingSystem  "{typeCodingSystem}" ;
            fhir:typeCodingCode "{typeCodingCode}" ;
            fhir:typeCodingDisplay "{typeCodingDisplay}" ;"""
        return coding_line

    def get_identifier_with_reference(identifier):
        identifierSystem = identifier[0].get('system', "") if identifier else ""
        identifierValue = identifier[0].get('value', "") if identifier else ""
        useLine = f"\n\t\t\tfhir:identifierUse \"{identifier[0]['use']}\" ;" if identifier[0].get('use') else ""
        assigner = f"\n\t\t\tfhir:assignerReference se:{split_refrence(identifier[0]['assigner']['reference'])} ;" if identifier[0].get('assigner') else ""
        identifier_line = f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{identifierValue}";{useLine}{assigner}"""
        return identifier_line  

    def get_hospitalization(hospital):
        if hospital == "":
            return ""
        hos = f"""\t\t\tfhir:admitSourceSystem "{hospital['admitSource']['coding'][0]['system']}"; 
            fhir:admitSourceCode "{hospital['admitSource']['coding'][0]['code']}" ;""" if hospital.get('admitSource') else ""
        
        pital = f"""\n\t\t\tfhir:dischargeSourceSystem "{hospital['dischargeDisposition']['coding'][0]['system']}"; 
            fhir:dischargeSourceCode "{hospital['dischargeDisposition']['coding'][0]['code']}" ;""" if hospital.get('dischargeDisposition') else ""
        
        return hos + pital
    def get_serviceType(service):
        if len(service) == 0:
            return ""
        
        service_line = f"""\t\t\tfhir:serviceTypeCodingSystem "{service['coding'][0]['system']}" ;
            fhir:serviceTypeCodingCode "{service['coding'][0]['code']}" ;"""
        return service_line  
    def get_priority(priority):
        if len(priority) == 0:
            return ""
        
        priority_line = f"""\t\t\tfhir:priorityCodingSystem "{priority['coding'][0]['system']}";
            fhir:priorityCodingValue "{priority['coding'][0]['system']}";"""
        return priority_line 
    def get_period(period):
        period_line = f"""\t\t\tfhir:periodStart "{period['start']}" ; 
            fhir:periodEnd "{period['end']}" """
        return period_line
    def get_location(location):
        locations = ""
        a=0
        location_list=""
        location_line=""
        for x in location:
            id=generate_id("locationEncounter"+str(a))
            reference = split_refrence(x['location']['reference'])
            period = get_period(x['period'])
            location_list=location_list+f"\t\t\tfhir:LocationEncounterReference se:{id} ;\n"
            location_line = location_line+f"""\nse:{id} a fhir:LocationEncounter ;
            fhir:locationEncounterReference se:{reference} ;
{period} .

"""
            a+=1
        return location_list,location_line
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
        hospitalization = get_hospitalization(result.get('hospitalization', ""))
        partOf = f"fhir:partOfReference se:{split_refrence(result['partOf']['reference'])} ;" if result.get("partOf") else ""
        serviceProvider = f"fhir:serviceProviderReference se:{split_refrence(result['serviceProvider']['reference'])} ;" if result.get('serviceProvider') else ""
        serviceType = get_serviceType(result.get('serviceType', []))
        priority = get_priority(result.get('priority', []))
        periodLine = get_period(result.get('period'))
        location_list,location_line = get_location(result.get('location', ''))
        type_line = get_coding(result.get('type', []))   
        insert = f"""se:{fhirID} a fhir:Encounter ;
            fhir:id "{fhirID}" ;
            fhir:classSystem "{result['class']['system']}";
            fhir:classCode "{result['class']['code']}";
            fhir:status "{result['status']}" ;
{periodLine} ;
            {partOf}
            {serviceProvider}
{type_line}
{serviceType}
{priority}
{location_list}
{identifier}
{hospitalization}
            fhir:subjectReference se:{patient}  .
        
"""
        write_to_middle(insert+location_line)
    move_to_final()
    time_end = time.time()
    print(f"encounter entity creation took {time_end - time_start:.4f} seconds")
    
def create_procedure_entities():
    print("creating procedure entities")
    time_start = time.time()
    def get_identifier(identifier):
        """
        This fuction converts identifier information into knowledge graph format
        Args:
            identifier (dict): a dictionary holding coding information
        Returns:
            str: the converted identifier property 
        """
        if len(identifier)==0:
            return ""
        identifierSystem=identifier[0].get('system',"")  if identifier else ""
        identifierValue=identifier[0].get('value',"")  if identifier else ""
        identifier_line=f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{sanatize_quotes(identifierValue)}" ;"""
        return identifier_line
    def get_category(cat):
        if len(cat) == 0:
            return ""
        
        catCode = cat['coding'][0]['code']
        catSystem = cat['coding'][0]['system']
        return f"""\t\t\tfhir:categoryCodingCode "{catCode}" ;
            fhir:categoryCodingSystem "{catSystem}" ; 
"""   
    def get_period(per):
        if len(per) == 0:
            return ""
        
        start = per['start']
        end = per['end']
        return f"""\t\t\tfhir:performedPeriodStart "{start}" ;
            fhir:performedPeriodEnd "{end}" ; 
"""   
    def get_body(body):
        if len(body) == 0:
            return ""
        
        catCode = body[0]['coding'][0]['code']
        catSystem = body[0]['coding'][0]['system']
        return f"""fhir:bodySiteSystem "{catSystem}" ;
            fhir:bodySiteCode "{catCode}" ; 
"""
    
    def get_coding(codinga):
        """
        This fuction converts coding information into knowledge graph format
        Args:
            coding (dict): a dictionary holding coding information
        Returns:
            str: the converted coding property 
        """
        coding=codinga['coding']
        if len(coding)==0:
            return ""
        codeCodingSystem = coding[0].get('system', '')
        codeCodingCode = coding[0].get('code', '')
        codeCodingDisplay = coding[0].get('display', '')
        coding_line =f"""\t\t\tfhir:codeCodingSystem  "{codeCodingSystem}" ;
            fhir:codeCodingCode "{codeCodingCode}" ;
            fhir:codeCodingDisplay "{codeCodingDisplay}" ;"""
        return coding_line

    
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
        coding_list = get_coding(result['code'])
        category_list = get_category(result.get('category', []))
        performedDateTime = f"\t\t\tfhir:performedDateTime \"{result.get('performedDateTime')}\" ;" if result.get('performedDateTime') else ""
        performedPeriod = get_period(result.get('performedPeriod', []))
        bodySite = get_body(result.get('bodySite', []))
        
        insert = f"""se:{fhirID} a fhir:Procedure;
            fhir:id "{fhirID}" ;
            fhir:status "{status}" ;
            fhir:encounterReference se:{encounter_reference} ;
{identifier}
{coding_list}
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
        identifierSystem = result['identifier'][0]['system']
        identifierValue = result['identifier'][0]['value']
        cat = result['category'][0]['coding'][0]
        catCode = cat['code']
        catSystem = cat['system']
        codeCodingCode = result['code']['coding'][0]['code']
        codeCodingDisplay = result['code']['coding'][0]['display']
        codeCodingSystem = result['code']['coding'][0]['system']
        patient = split_refrence(result['subject']['reference'])
        encounter = split_refrence(result['encounter']['reference'])
        
        insert = f"""se:{fhirID} a fhir:Condition;
            fhir:id "{fhirID}" ;
            fhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{identifierValue}" ;
            fhir:categoryCodingCode "{catCode}" ;
            fhir:categoryCodingSystem "{catSystem}" ;
            fhir:codeCodingCode "{codeCodingCode}" ;
            fhir:codeCodingDisplay "{codeCodingDisplay}" ;
            fhir:codeCodingSystem "{codeCodingSystem}" ;
            fhir:encounterReference se:{encounter} ;
            fhir:subjectReference se:{patient} . 
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"condition entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def get_dosageInstruction_entities(dosaga,id):
    if len(dosaga)==0:
        return ""
    dosage=dosaga[0]
    dar=dosage['doseAndRate'] if dosage.get('doseAndRate',None) else None
    doseQuantityCode=f"\n\t\t\tfhir:doseQuantityCode \"{dosage['doseAndRate'][0]['doseQuantity']['code']}\" ;" if dar else ""
    doseQuantitySystem=f"\n\t\t\tfhir:doseQuantitySystem \"{dosage['doseAndRate'][0]['doseQuantity']['system']}\" ;" if dar else ""
    doseQuantityUnit=f"\n\t\t\tfhir:doseQuantityUnit \"{dosage['doseAndRate'][0]['doseQuantity']['unit']}\" ;" if dar else ""
    doseQuantityValue=f"\n\t\t\tfhir:doseQuantityValue \"{dosage['doseAndRate'][0]['doseQuantity']['value']}\" ;" if dar else ""
    mdpp=dosage['maxDosePerPeriod'] if dosage.get('maxDosePerPeriod',None) else None
    maxDosePerPeriodDenominatorSystem=f"\n\t\t\tfhir:maxDosePerPeriodDenominatorSystem \"{mdpp['denominator']['system']}\" ;" if mdpp else ""
    maxDosePerPeriodDenominatorUnit=f"\n\t\t\tfhir:maxDosePerPeriodDenominatorUnit \"{mdpp['denominator']['unit']}\" ;" if mdpp else ""
    maxDosePerPeriodDenominatorValue=f"\n\t\t\tfhir:maxDosePerPeriodDenominatorValue \"{mdpp['denominator']['value']}\" ;" if mdpp else ""
    maxDosePerPeriodNumeratorValue=f"\n\t\t\tfhir:maxDosePerPeriodNumeratorValue \"{mdpp['numerator']['value']}\" ;" if mdpp else ""
    timing=dosage['timing'] if dosage.get('timing',None) else None
    timingCodeCodingCode=f"\n\t\t\tfhir:timingCodeCodingCode \"{timing['code']['coding'][0]['code']}\" ;" if timing else ""
    timingCodeCodingSystem=f"\n\t\t\tfhir:timingCodeCodingSystem \"{timing['code']['coding'][0]['system']}\" ;" if timing else ""
    repeat=timing['repeat'] if timing and timing.get('repeat',None) else None
    timingRepeatDuration = f"\n\t\t\tfhir:timingRepeatDuration \"{repeat['duration']}\" ;" if repeat else ""
    timingRepeatDurationUnit = f"\n\t\t\tfhir:timingRepeatDurationUnit \"{repeat['durationUnit']}\" ;" if repeat else ""
    optional=f"{doseQuantityCode}{doseQuantitySystem}{doseQuantityUnit}{doseQuantityValue}{maxDosePerPeriodDenominatorSystem}"
    optional=f"{optional}{maxDosePerPeriodDenominatorUnit}{maxDosePerPeriodDenominatorValue}{maxDosePerPeriodNumeratorValue}"
    optional=f"{optional}{timingCodeCodingCode}{timingCodeCodingSystem}{timingRepeatDuration}{timingRepeatDurationUnit}"
    return f"""se:{id} a fhir:DosageInstruction ;
            fhir:routeCodingCode "{dosage['route']['coding'][0]['code']}" ;{optional}
            fhir:routeCodingSystem "{dosage['route']['coding'][0]['system']}" .

"""

def create_medicationDispense_entities():
    print("creating medication dispense entities")
    time_start = time.time()
    
    def get_identifier(identifier):
        """
        This fuction converts identifier information into knowledge graph format
        Args:
            identifier (dict): a dictionary holding coding information
        Returns:
            str: the converted identifier property 
        """
        if len(identifier)==0:
            return ""
        identifierSystem=identifier[0].get('system',"")  if identifier else ""
        identifierValue=identifier[0].get('value',"")  if identifier else ""
        identifier_line=f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{sanatize_quotes(identifierValue)}" ;"""
        return identifier_line

    def get_small_coding(coding):
        """
        This fuction converts coding information into knowledge graph format without display
        Args:
            coding (dict): a dictionary holding coding information
        Returns:
            str: the converted coding property 
        """
        if len(coding)==0:
            return ""
        typeCodingSystem = coding[0].get('system', '')
        typeCodingCode = coding[0].get('code', '')
        coding_line =f"""\t\t\tfhir:mccCodingCode "{typeCodingCode}" ;
            fhir:mccCodingSystem "{typeCodingSystem}" ; 
"""
        return coding_line

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
        mccCoding = get_small_coding(result['medicationCodeableConcept']['coding'])
        status = result['status']
        dosageID=generate_id("md dosage"+fhirID)
        dosage = get_dosageInstruction_entities(result.get('dosageInstruction',[]),dosageID)
        dosageLine=f"\n\t\t\tfhir:dosageInstructionReference se:{dosageID} ;" if len(dosage)!=0 else ""
        insert = f"""se:{fhirID} a fhir:MedicationDispense;
            fhir:id "{fhirID}" ;
{identifier}
            fhir:contextReference se:{context} ;
            fhir:authorizingPrescriptionReference se:MR-{authorizingPrescription} ;
{mccCoding}
            fhir:status "{status}" ;{dosageLine} 
            fhir:subjectReference se:{subject} .
        
"""
        write_to_middle(insert+dosage)
    time_end = time.time()
    print(f"medication dispense entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medicationRequest_entities():
    def get_dispense_request(dispense):
        if len(dispense) == 0:
            return ""
        return f"""fhir:dispenseRequestValidityPeriodStart "{dispense['validityPeriod']['start']}" ;
            fhir:dispenseRequestValidityPeriodEnd "{dispense['validityPeriod']['end']}" ;
"""


    def get_mr_identifier(identifier):
        return f"""fhir:identifierSystem "{identifier[0]['system']}" ;
            fhir:identifierValue "{identifier[0]['value']}" ;
            fhir:identifierTypeCodingSystem "{identifier[0]['type']['coding'][0]['system']}" ;
            fhir:identifierTypeCodingCode "{identifier[0]['type']['coding'][0]['code']}" ;
            fhir:identifierTypeCodingDisplay "{identifier[0]['type']['coding'][0]['display']}" ;
"""

    def get_mcc(codinga):
        if len(codinga)==0:
            return ""
        coding=codinga['coding']
        typeCodingSystem = coding[0].get('system', '')
        typeCodingCode = coding[0].get('code', '')
        coding_line =f"""\t\t\tfhir:mccCodingCode "{typeCodingCode}" ;
            fhir:mccCodingSystem "{typeCodingSystem}" ; 
"""
        return coding_line

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
        authoredOn = result['authoredOn']
        subject = split_refrence(result['subject']['reference'])
        encounter = split_refrence(result['encounter']['reference'])
        dispenseRequest = get_dispense_request(result.get('dispenseRequest', []))
        dosageID=generate_id("mr dosage"+fhirID)
        dosage = get_dosageInstruction_entities(result.get('dosageInstruction',[]),dosageID)
        dosageLine=f"\n\t\t\tfhir:dosageInstructionReference se:{dosageID} ;" if len(dosage)!=0 else ""
        identifier = get_mr_identifier(result['identifier'])
        mcc = get_mcc(result.get('medicationCodeableConcept', []))
        intent = result['intent']
        status = result['status']
        medication = f"fhir:medicationReference se:{split_refrence(result['medicationReference']['reference'])} ;" if len(result.get('medicationReference', [])) > 0 else ""
        insert = f"""se:MR-{fhirID} a fhir:MedicationRequest ;
            fhir:id "{fhirID}" ;
            fhir:encounterReference se:{encounter}  ;
            fhir:subjectReference se:{subject}  ;
            fhir:intent "{intent}" ;
            fhir:status "{status}" ;
            {medication}
            {dispenseRequest}{dosageLine}
            {identifier}
{mcc}
            fhir:authoredOn "{authoredOn}" .
        
"""
        write_to_middle(insert+dosage)
    time_end = time.time()
    print(f"medication request entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_specimen_entities():
    def get_specimen_type(typa):
        display=f"\n\t\t\tfhir:typeCodingDisplay  \"{typa['coding'][0]['display']}\" ;" if typa['coding'][0].get('display') else ""
        return f"""\t\t\tfhir:typeCodingSystem "{typa['coding'][0]['system']}" ;
            fhir:typeCodingCode "{typa['coding'][0]['code']}" ;{display}"""
    def get_identifier(identifier):
        """
        This fuction converts identifier information into knowledge graph format
        Args:
            identifier (dict): a dictionary holding coding information
        Returns:
            str: the converted identifier property 
        """
        if len(identifier)==0:
            return ""
        identifierSystem=identifier[0].get('system',"")  if identifier else ""
        identifierValue=identifier[0].get('value',"")  if identifier else ""
        identifier_line=f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{sanatize_quotes(identifierValue)}" ;"""
        return identifier_line
    time_start = time.time()
    print("creating specimen entities")
    pipeline = [
        {"$match":{"resourceType":"Specimen"}},
        {"$project":{"_id":1,"id":1,"identifier":1,"collection":1,"type":1,"subject":1,"meta":1}}
    ]
    results = collection.aggregate(pipeline)
    hashset=set()
    for result in results:
        fhirID=result.get('id')
        if fhirID in hashset:
            continue
        hashset.add(fhirID)
        identifier = get_identifier(result.get('identifier', []))
        type_list = get_specimen_type(result.get('type', []))
        subject = split_refrence(result['subject']['reference'])
        collectedDateTime = f"\t\t\tfhir:collectedDateTime \"{result['collection']['collectedDateTime']}\" ;" if len(result.get('collection',[]))!=0 else ""
        insert =f"""se:{fhirID} a fhir:Specimen ;
            fhir:id "{fhirID}" ;
{identifier}
{type_list}
{collectedDateTime}
            fhir:subjectReference se:{subject} .
        
"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"specimen entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medication_entities():
    def get_medication_identifier(identifier):
        identifiers=""
        for x in identifier:
            identifiers=identifiers+f"""\t\t\tfhir:identifierSystem "{x['system']}" ;
            fhir:identifierValue "{escape_turtle_string(x['value'])}" ;
"""
        return identifiers
    def get_ingredients(ing):
        if len(ing)==0:
            return ""
        ingredients=""
        for x in ing:
            ingredients=ingredients + f"\t\t\tfhir:ingredientReference se:{split_refrence(x['itemReference']['reference'])} ;\n"
        return ingredients
    def get_medication_code(coda):
        if len(coda)==0:
            return ""
        return f"""\t\t\tfhir:codeCodingSystem "{coda['coding'][0]['code']}";
            fhir:codeCodingCode "{coda['coding'][0]['code']}" ;"""
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
        ingredients=get_ingredients(result.get('ingredient',[]))
        insert =f"""se:{fhirID} a fhir:Medication ;
{identifier}
{ingredients}
{code}
            fhir:id "{fhirID}" .

"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"medication entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_medicationAdministration_entities():
    def get_category(cat):
        if len(cat)==0:
            return ""
        return f"""\t\t\tfhir:categoryCodingSystem "{cat['coding'][0]['system']}" ;
            fhir:categoryCodingCode "{cat['coding'][0]['code']}"; """
    def get_dosage(dosage):
        if len(dosage)==0:
            return ""
        text = f"\t\t\tfhir:doseText \"{dosage['text']}\" ;\n" if dosage.get('text',None) else ""
        rateQuantity = f"""\t\t\tfhir:dosageRateQuantitySystem "{dosage['rateQuantity']['system']}" ;
            fhir:dosageRateQuantityUnit "{dosage['rateQuantity']['unit']}" ;
            fhir:dosageRateQuantityValue "{dosage['rateQuantity']['value']}" ;
            fhir:dosageRateCode "{dosage['rateQuantity']['code']}" ;
""" if dosage.get('rateQuantity',None) else ""
        method = f"""\t\t\tfhir:methodCodingsystem "{dosage['method']['coding'][0]['system']}" ;
            fhir:methodCodingcode "{dosage['method']['coding'][0]['code']}" ;
""" if dosage.get('method',None) else ""
        dose_code=f"\n\t\t\tfhir:doseCode \"{escape_turtle_string(dosage['dose']['code'])}\" ;" if dosage['dose'].get('code') else ""
        dose_unit=f"\n\t\t\tfhir:doseUnit \"{escape_turtle_string(dosage['dose']['unit'])}\" ;" if dosage['dose'].get('unit') else ""
        dose = f"""\t\t\tfhir:doseSystem "{dosage['dose']['system']}" ;
            fhir:doseValue "{dosage['dose']['value']}" ;{dose_code}{dose_unit}
"""
        return f"""{rateQuantity}{text}{method}{dose}
"""
    def get_period(period):
        if len(period)==0:
            return ""
        period_line = f"""\t\t\tfhir:effectivePeriodStart "{period['start']}" ;
            fhir:effectivePeriodEnd "{period['end']}" ;"""
        return period_line
    def get_medication_code(coda):
        if len(coda)==0:
            return ""
        display=f" ;\n\t\t\tfhir:codeCodingDisplay \"{coda['coding'][0]['display']}\" ;" if coda['coding'][0].get('display',None) else ""
        return f"""\t\t\tfhir:codeCodingSystem "{coda['coding'][0]['code']}" ;
                    fhir:codeCodingCode "{coda['coding'][0]['code']}" ; {display}
"""
    def get_mr_identifier(identifier):
        return f"""fhir:identifierSystem "{identifier[0]['system']}" ;
            fhir:identifierValue "{identifier[0]['value']}" ;
            fhir:identifierTypeCodingSystem "{identifier[0]['type']['coding'][0]['system']}" ;
            fhir:identifierTypeCodingCode "{identifier[0]['type']['coding'][0]['code']}" ;
            fhir:identifierTypeCodingDisplay "{identifier[0]['type']['coding'][0]['display']}" ;
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
        effectiveDateTime =f"\t\t\tfhir:effectiveDateTime \"{result['effectiveDateTime']}\" ;" if len(result.get('effectiveDateTime',[])) else ""
        request =f"\t\t\tfhir:requestReference se:{split_refrence(result['request']['reference'])} ;" if len(result.get('request',[]))!=0 else ""
        context =f"\t\t\tfhir:contextReference se:{split_refrence(result['context']['reference'])} ;" if len(result.get('context',[]))!=0 else ""
        category=get_category(result.get('category',[]))
        dosage=get_dosage(result.get('dosage',[]))
        period=get_period(result.get('effectivePeriod',[]))
        mcc=get_medication_code(result['medicationCodeableConcept'])
        status=result['status']
        subject=split_refrence(result['subject']['reference'])
        identifer=get_mr_identifier(result['identifier'])
        insert =f"""se:{fhirID} a fhir:MedicationAdministration ;
            fhir:id "{fhirID}" ;
{request}
{mcc}
    {identifer}
{category}
{context}
{period}
{effectiveDateTime}
{dosage}
            fhir:subjectReference se:{subject} ; 
            fhir:status "{status}" .

"""
        write_to_middle(insert)
    time_end = time.time()
    print(f"medication administration entity creation took {time_end - time_start:.4f} seconds")
    move_to_final()

def create_observation_entities():
    def get_category(cat):
        return f"""\t\t\tfhir:categoryCodingSystem "{cat[0]['coding'][0]['system']}" ;
            fhir:categoryCodingCode "{cat[0]['coding'][0]['code']}" . """
    def get_o_code(code):
        return f"""\t\t\tfhir:codeCodingSystem "{code['coding'][0]['system']}";
            fhir:codeCodingCode "{code['coding'][0]['system']}" ;
            fhir:codeCodingDisplay "{code['coding'][0]['display']}" ;"""
    def get_extension(ex):
        if len(ex)==0:
            return ""
        comparator=f"\n\t\t\tfhir:extensionValueQuantityComparator \"{ex[0]['valueQuantity']['comparator']}\" ;" if len(ex[0].get('valueQuantity',[]))!=0 and len(ex[0]['valueQuantity'].get('comparator',[]))!=0 else ""
        valueQuantity=f"""\n\t\t\tfhir:extensionValueQuantityValue "{ex[0]['valueQuantity']['value']}" ;{comparator}
""" if len(ex[0].get('valueQuantity',[]))!=0 else ""
        valueString=f"\n\t\t\tfhir:extensionValueString \"{ex[0]['valueString']}\" ;" if len(ex[0].get('valueString',[]))!=0 else ""
        return f"""fhir:extensionUrl "{ex[0]['url']}" ;{valueString}{valueQuantity}"""
    def get_members(members):
        if len(members)==0:
            return ""
        references=""
        for x in members:
            references=references+f"\t\t\tfhir:hasMemberReference se:{split_refrence(x['reference'])} ;\n"
        return references
    def get_interpretation(inter):
        if len(inter)==0:
            return ""
        return f"""\t\t\tfhir:interpretationCodingSystem "{inter[0]['coding'][0]['system']}" ;
            fhir:interpretationCodingCode "{inter[0]['coding'][0]['code']}" ; """
    def get_note(note):
        if len(note)==0:
            return ""
        return f"""\t\t\tfhir:noteText "{sanitize_for_kg_literal(note[0]['text'])}" ;"""
    def get_referenceRange(rr):
        if len(rr)==0:
            return ""
        high_unit=f"\n\t\t\tfhir:rrHighUnit \"{rr[0]['high']['unit']}\" ;" if len(rr[0].get('high',[]))!=0 and rr[0]['high'].get('unit',None) else ""
        high_value=f"\n\t\t\tfhir:rrHighValue \"{rr[0]['high']['value']}\" ;" if len(rr[0].get('high',[]))!=0 and rr[0]['high'].get('value',None) else ""
        high_code=f"\n\t\t\tfhir:rrHighCode \"{rr[0]['high']['code']}\" ;" if len(rr[0].get('high',[]))!=0 and rr[0]['high'].get('code',None) else ""
        high=f"""\t\t\tfhir:rrHighSystem \"{rr[0]['high']['system']}\" ;{high_unit}{high_value}{high_code}
""" if len(rr[0].get('high',[]))!=0 else ""
        
        low_unit=f";\n\t\t\tfhir:rrLowUnit \"{rr[0]['low']['unit']}\" ;" if len(rr[0].get('low',[]))!=0 and rr[0]['low'].get('unit',None) else ""
        low_value=f";\n\t\t\tfhir:rrLowValue \"{rr[0]['low']['value']}\" ;" if len(rr[0].get('low',[]))!=0 and rr[0]['low'].get('value',None) else ""
        low_code=f";\n\t\t\tfhir:rrLowCode \"{rr[0]['low']['code']}\" ;" if len(rr[0].get('low',[]))!=0 and rr[0]['low'].get('code',None) else ""
        low=f"""\t\t\tfhir:rrLowSystem \"{rr[0]['low']['system']}\" ;{low_unit}{low_value}{low_code}""" if len(rr[0].get('low',[]))!=0 else ""
        return high+low
    def get_vcc(vcc):
        if len(vcc)==0:
            return ""
        display=f";\n\t\t\tfhir:vccCodingDisplay \"{vcc['coding'][0]['display']}\" ;" if vcc['coding'][0].get('display',None) else ""
        return f"""\t\t\tfhir:vccCodingSystem"{vcc['coding'][0]['system']}" ;
                    fhir:vccCodingCode "{vcc['coding'][0]['code']}" ;"""
    def get_valueQuantity(vq):
        if len(vq)==0:
            return ""
        value =f"\n\t\t\tfhir:valueQuantityValue \"{vq['value']}\" ;" if vq.get('value',None) else ""
        comparator =f"\n\t\t\tfhir:valueQuantityComparator \"{vq['comparator']}\" ;" if vq.get('comparator',None) else ""
        code = f";\n\t\t\tfhir:valueQuantityCode \"{vq['code']}\" ;" if vq.get('code',None) else ""
        return f"""\t\t\tfhir:valueQuantitySystem "{vq['system']}" ;{value}{comparator}{code}"""
    def get_identifier(identifier):
        """
        This fuction converts identifier information into knowledge graph format
        Args:
            identifier (dict): a dictionary holding coding information
        Returns:
            str: the converted identifier property 
        """
        if len(identifier)==0:
            return ""
        identifierSystem=identifier[0].get('system',"")  if identifier else ""
        identifierValue=identifier[0].get('value',"")  if identifier else ""
        identifier_line=f"""\t\t\tfhir:identifierSystem "{identifierSystem}" ;
            fhir:identifierValue "{sanatize_quotes(identifierValue)}" ;"""
        return identifier_line
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
        derivedFrom =f"\t\t\tfhir:derivedFromReference se:{split_refrence(result['derivedFrom'][0]['reference'])} ;" if len(result.get('derivedFrom',[]))!=0 else ""
        encounter =f"\t\t\tfhir:encounterReference se:{split_refrence(result['encounter']['reference'])} ;" if len(result.get('encounter',[]))!=0 else ""
        specimen =f"\t\t\tfhir:specimenReference se:{split_refrence(result['specimen']['reference'])} ;" if len(result.get('specimen',[]))!=0 else ""
        status = f"\t\t\tfhir:status \"{result['status']}\" ;"
        subject = f"\t\t\tfhir:subjectReference se:{split_refrence(result['subject']['reference'])} ;"
        effectiveDateTime=f"\t\t\tfhir:effectiveDateTime \"{result['effectiveDateTime']}\" ;" if result.get('effectiveDateTime',None) else ""
        extension=get_extension(result.get('extension',[]))
        identifier=get_identifier(result['identifier'])
        hasMember=get_members(result.get('hasMember',[]))
        interpretation=get_interpretation(result.get('interpretation',[]))
        issued=f"\t\t\tfhir:issued \"{result['issued']}\" ; " if result.get('issued', None) else ""
        valueDateTime=f"\t\t\tfhir:valueDateTime \"{result['valueDateTime']}\" ; " if result.get('valueDateTime', None) else ""
        valueString=f"\t\t\tfhir:valueString \"{sanatize_quotes(result['valueString'])}\" ; " if result.get('valueString', None) else ""
        note=get_note(result.get('note',[]))
        referenceRange=get_referenceRange(result.get('referenceRange',[]))
        vcc=get_vcc(result.get('valueCodeableConcept',[]))
        valueQuantity=get_valueQuantity(result.get('valueQuantity',[]))
        insert =f"""se:{fhirID} a fhir:Observation ;
            fhir:id "{fhirID}" ;
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
