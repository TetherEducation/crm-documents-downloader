from data_processing import load_and_process_excel, map_stage, check_stage_empty
from mongo_operations import fetch_admissions_data, fetch_surveys_data
from s3_operations import get_s3_client, download_from_answers
import os 
from pymongo import MongoClient
import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
load_dotenv()

input_dir = "/Users/leidygomez/Downloads/inputs_matricula_digital/"
output_dir = "/Users/leidygomez/Downloads/documentos_matricula_digital/"
MONGO_URI = os.getenv("MONGO_URI")
bucket = "crm-surveys-files"


carpetas = ["931800001", "1471700001"]

template_types = [
    'documents-uploader-full',
    'legalguardian-identification-uploader-survey',
    'responsible-adult-identification-uploader-survey',
    'social-household-registry-uploader-survey',
    'student-birthdate-uploader-survey',
    'student-identification-uploader-survey',
    'student-picture-uploader-survey',
    'student-previous-studies-uploader-survey',
    'substitute-legalguardian-identification-uploader-survey'
]
    
key_mapping = {
        "student_id_picture_frontside": "estudiante_id_frente",
        "student_id_picture_backside": "estudiante_id_reverso",
        "legalguardian_id_picture_backside": "apoderado_id_reverso",
        "legalguardian_id_picture_frontside": "apoderado_id_frente",
        "social_household_registry": "registro_social_hogares",
        "student_birth_certificate": "estudiante_certificado_nacimiento",
        "student_picture": "estudiante_foto",
        "responsible_adult_id_picture_backside": "adulto_responsable_id_reverso",
        "responsible_adult_id_picture_frontside": "adulto_responsable_id_frente",
        "student_previous_studies_certificate": "certificado_estudios_previos",
}

alertas = []

for carpeta in carpetas:
    df = load_and_process_excel(f"{input_dir}/{carpeta}/Ficha matrícula digital simple.xlsx")
    df = map_stage(df)
    alerta_stage = check_stage_empty(df, carpeta)
    if alerta_stage:
        alertas.append(alerta_stage)

    try:
        # Conectando a admissions
        client = MongoClient(MONGO_URI)
        admission_id_strings = df['admission_id'].tolist()
        admission_ids = [ObjectId(id_str) for id_str in admission_id_strings]
        admissions = fetch_admissions_data(client, "crm", "admissions", admission_ids)
        admissions = pd.DataFrame(admissions).rename(columns={'_id': 'admission_id'}).astype(str)

        if admissions.empty:
            alerta_admissions = f"Alerta: La base de admissions está vacía para la carpeta {carpeta}"
            print(alerta_admissions)
            alertas.append(alerta_admissions)
            continue  # Saltar a la siguiente carpeta

        # Conectando a surveys
        applicationIds = admissions['applicationId'].tolist()
        surveys = fetch_surveys_data(client, "tools", "surveys", applicationIds, template_types)
        surveys = pd.DataFrame(surveys).rename(columns={'_id': 'survey_id', 'externalId': 'applicationId'})

        if surveys.empty:
            alerta_surveys = f"Alerta: La base de surveys está vacía para la carpeta {carpeta}"
            print(alerta_surveys)
            alertas.append(alerta_surveys)
            continue  # Saltar a la siguiente carpeta

        # Uniendo dataframes
        surveys = pd.merge(surveys, admissions, on='applicationId', how='left')
        surveys = pd.merge(surveys, df, on='admission_id', how='left')

        # Descargando los archivos desde S3
        s3_client = get_s3_client()
        for _, row in surveys.iterrows():
            download_from_answers(
                row["answers"], 
                row["rut"], 
                row["nombre_estudiante"], 
                carpeta, 
                row["stage"], 
                row["Nivel"], 
                row["Jornada"], 
                output_dir, 
                key_mapping, 
                bucket, 
                s3_client
            )
        print(f"Descarga completada para campus_code {carpeta}.")

        # Guardar alertas en un archivo de texto
        if alertas:
            with open(output_dir + "alertas.txt", "w") as archivo:
                archivo.write("\n\n".join(alertas))
            print("Se han guardado las alertas en el archivo 'alertas.txt'.")

    except Exception as e:
        alerta_error = f"Error al procesar la carpeta {carpeta}: {e}"
        print(alerta_error)
        alertas.append(alerta_error)