
def fetch_admissions_data(client, database, collection_name, admission_ids, batch_size=1000):
    db = client[database]
    collection = db[collection_name]
    admissions_data = []
    for i in range(0, len(admission_ids), batch_size):
        batch = admission_ids[i:i + batch_size]
        cursor = collection.find({"_id": {"$in": batch}}, {"_id": 1, "applicationId": 1})
        admissions_data.extend(list(cursor))
    return admissions_data


def fetch_surveys_data(client, database, collection_name, application_ids, template_types, batch_size=1000):
    db = client[database]
    collection = db[collection_name]
    surveys_data = []
    for i in range(0, len(application_ids), batch_size):
        batch = application_ids[i:i + batch_size]
        cursor = collection.find({
            "externalId": {"$in": batch},
            "operation": "crm-admission",
            "templateType": {"$in": template_types},
            "answers": {"$exists": True, "$ne": {}}
        }, {"_id": 1, "templateType": 1, "answers": 1, "externalId": 1})
        surveys_data.extend(list(cursor))
    return surveys_data


def fetch_contracts_data(client, database, collection_name, campus_code, application_ids, batch_size=1000):
    db = client[database]
    collection = db[collection_name]
    contracts_data = []
    for i in range(0, len(application_ids), batch_size):
        batch = application_ids[i:i + batch_size]
        cursor = collection.find({
        "externalId": {"$in": batch},
        "campusId": campus_code,
        "status": "SIGNED"},
        {"externalId": 1, "providerTemplateId": 1, "userId": 1, "campusId": 1})
        contracts_data.extend(list(cursor))
    return contracts_data


