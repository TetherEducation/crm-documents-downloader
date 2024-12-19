import requests
import pandas as pd
import os
import base64
from dotenv import load_dotenv
load_dotenv()

# Configura el Bearer Token para la autenticación
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
API_URL = "https://api.tether.education/contracts"

# Función para realizar la solicitud y descargar el contrato
def download_contract(template_id, external_id, user_id, campus_code, applicant_id, applicant_name, stage, nivel, jornada, output_path):
    query = """
    query DownloadContractByCampus {
        downloadContractByCampus(
            templateId: "%s"
            externalId: "%s"
        ) {
            filename
            contentType
            contentSource
            data
        }
    }
    """ % (template_id, external_id)

    # Validar token y encabezados
    if not BEARER_TOKEN:
        print("El Bearer Token no está configurado. Verifica tu archivo .env")
        return

    if not campus_code or not user_id:
        print(f"Faltan valores para campus_code o user_id para templateId={template_id} y externalId={external_id}")
        return

    # Encabezados de la solicitud
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
        "X-Tenant-Code": "cl",
        "X-Campus-Id": campus_code,
        "X-User-Id": user_id
    }

    try:
        # Realizar la solicitud POST
        response = requests.post(API_URL, json={"query": query}, headers=headers)
        response.raise_for_status()  # Lanzar excepción si la respuesta tiene errores

        # Procesar la respuesta
        data = response.json()
        contract = data.get("data", {}).get("downloadContractByCampus", {})

        if contract:
            # Extraer información del contrato
            contract_data = contract.get("data", "")
            if not contract_data:
                print(f"No hay datos para el contrato con templateId={template_id} y externalId={external_id}")
                return

            # Renombrar archivo
            new_file_name = f"contrato_{template_id}_{applicant_name}_{applicant_id}.pdf"
            file_content = base64.b64decode(contract_data)

            # Crear directorio de salida si no existe
            local_applicant_folder = os.path.join(output_path, campus_code, stage, nivel, jornada, f"{applicant_name}_{applicant_id}")
            os.makedirs(local_applicant_folder, exist_ok=True)

            # Guardar archivo en el sistema local
            local_file_path = os.path.join(local_applicant_folder, new_file_name)
            with open(local_file_path, "wb") as file:
                file.write(file_content)

            print(f"Contrato descargado: {local_file_path}")
        else:
            print(f"No se encontró contrato para templateId={template_id} y externalId={external_id}")

    except Exception as e:
        print(f"Error al descargar contrato para templateId={template_id} y externalId={external_id}: {e}")
