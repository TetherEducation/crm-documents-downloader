from data_processing import process_student_data, map_stage, check_stage_empty
from mongo_operations import fetch_surveys_data, fetch_contracts_data
from s3_operations import get_s3_client, download_from_answers
from contracts_operations import download_contract
from download_admission_form import download_admission_form
from dictionary import carpetas, template_types, key_mapping
import os 
import re
import datetime
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


output_dir = "/Users/leidygomez/Library/CloudStorage/GoogleDrive-leidy@tether.education/Shared drives/Matrícula Digital/Chile/"
output_dir_local = "/Users/leidygomez/Downloads/"
notas_file_path = os.path.join(output_dir_local, "notas_procesamiento.txt")

MONGO_URI = os.getenv("MONGO_URI")
bucket = "crm-surveys-files"

relacion_rbd_campus_path = os.path.join(os.path.dirname(__file__), "data/relacion_rbd_campus.csv")
relacion_rbd_campus = pd.read_csv(relacion_rbd_campus_path)

    
alertas = []
alertas_contratos = [] 

for carpeta in carpetas:
    # Extraer institution_code
    institution_code = re.match(r'^\d+', carpeta).group()
    print(f"Institution code: {institution_code}")

    # Obtener los campus_code asociados
    campus_codes = relacion_rbd_campus[relacion_rbd_campus['institution_code'] == institution_code]['campus_code'].tolist()
    print(f"Campus codes asociados: {campus_codes}")

    # Iterar sobre los campus_code
    for campus_code in campus_codes:
        print(f"Procesando campus_code: {campus_code}")

        try:
            df = download_admission_form(campus_code)
            df = process_student_data(df)
            df = map_stage(df)

            #Verificar si hay stage vacío
            alerta_stage = check_stage_empty(df, campus_code)
            if alerta_stage:
                alertas.append(alerta_stage)
        
            # Conectando a surveys
            client = MongoClient(MONGO_URI)
            applicationIds = df['applicationId'].tolist()
            surveys = fetch_surveys_data(client, "tools", "surveys", applicationIds, template_types)
            surveys = pd.DataFrame(surveys).rename(columns={'_id': 'survey_id', 'externalId': 'applicationId'})

            if surveys.empty:
                alerta_surveys = f"Alerta: La base de surveys está vacía para campus_code {campus_code}"
                print(alerta_surveys)
                alertas.append(alerta_surveys)
            else:
                # Uniendo dataframes y descargando archivos de S3
                surveys = pd.merge(surveys, df, on='applicationId', how='left')
                surveys = surveys.sort_values(by=['stage', 'Nivel', 'Jornada', 'applicationId', 'templateType'], ascending=[True, True, True, True, True]).reset_index(drop=True)

                #Descargando los archivos desde S3
                s3_client = get_s3_client()
                for index, row in surveys.iterrows():
                    print(f"{index}/{len(surveys)}")
                    download_from_answers(
                        row["answers"], 
                        row["rut"], 
                        row["nombre_estudiante"], 
                        carpeta,  # Usar carpeta completa como nombre
                        row["stage"], 
                        row["Nivel"], 
                        row["Jornada"], 
                        output_dir, 
                        key_mapping, 
                        bucket, 
                        s3_client
                    )
                print(f"Descarga de S3 completada para campus_code {campus_code}.")
                os.makedirs(output_dir_local + f"{campus_code}/", exist_ok=True)
                surveys.to_csv(output_dir_local + f"{campus_code}/surveys.csv")

            # Conectando a contracts
            contracts = fetch_contracts_data(client, "tools", "contracts", campus_code, applicationIds)
            contracts = pd.DataFrame(contracts).rename(columns={'externalId': 'applicationId'})

            if contracts.empty:
                alerta_contracts = f"Alerta: La base de contracts está vacía para campus_code {campus_code}"
                print(alerta_contracts)
                alertas.append(alerta_contracts)
            else:
                # Uniendo dataframes y descargando contratos de pandadoc
                contracts = pd.merge(contracts, df, on='applicationId', how='left')
                contracts = contracts.sort_values(by=['stage', 'Nivel', 'Jornada', 'applicationId', 'providerTemplateId'], ascending=[True, True, True, True, True]).reset_index(drop=True)

                for index, row in contracts.iterrows():
                    print(f"{index}/{len(contracts)}")
                    download_contract(
                        row["providerTemplateId"], 
                        row["applicationId"], 
                        row["userId"], 
                        row["campusId"], 
                        carpeta,
                        row["rut"], 
                        row["nombre_estudiante"], 
                        row["stage"], 
                        row["Nivel"], 
                        row["Jornada"],
                        output_dir,
                        alertas
                    )
                print(f"Descarga de contratos completada para campus_code {campus_code}.")
                os.makedirs(output_dir_local + f"{campus_code}/", exist_ok=True)
                contracts.to_csv(output_dir_local + f"{campus_code}/contracts.csv")

            #Registrar notas para el campus actual
            with open(notas_file_path, "a") as notas_file:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                notas_file.write(f"\n\nProcesamiento completado para campus_code {campus_code} ({timestamp})\n")
                notas_file.write("Alertas encontradas:\n")
                notas_file.write("\n".join(alertas if alertas else ["Sin alertas."]))
                notas_file.write("\n")

        except Exception as e:
            alerta_error = f"Error al procesar la carpeta {carpeta} y campus_code {campus_code}: {e}"
            print(alerta_error)
            alertas.append(alerta_error)
            with open(notas_file_path, "a") as notas_file:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                notas_file.write(f"\n\nError procesando campus_code {campus_code} ({timestamp}): {alerta_error}\n")

