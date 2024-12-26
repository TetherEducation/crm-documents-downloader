import requests
import base64
import os
import pandas as pd


# Configura el Bearer Token para la autenticación
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
API_URL = "https://api.tether.education/crm/download"

# Función para realizar la solicitud y descargar el contrato
def download_individual_report(admissionId, campus_code, carpeta, applicant_id, applicant_name, stage, nivel, jornada, output_path):
    query = """
    query DownloadAdmissionCompletedReport {
        downloadAdmissionCompletedReport(admissionID: "%s") {
            filename
            contentType
            contentSource
            data
        }
    }
    """ % (admissionId)

    # Headers de la solicitud
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
        "X-Tenant-Code": "cl",
        "X-Campus-Id": campus_code
    }

    # Hacer la solicitud POST
    try:
        response = requests.post(API_URL, json={"query": query}, headers=headers)
        response.raise_for_status()  # Lanza una excepción si la respuesta es un error HTTP
        
        # Manejo de la respuesta
        if response.status_code == 200:
            response_data = response.json()
            report = response_data.get("data", {}).get("downloadAdmissionCompletedReport", {})
            new_file_name = f"ficha_{applicant_name}_{applicant_id}.pdf"
            pdf_data = report.get("data")
            
            # Crear directorio de salida si no existe
            local_applicant_folder = os.path.join(output_path, carpeta, "Matrícula 2025", stage, nivel, jornada, f"{applicant_name}_{applicant_id}")
            os.makedirs(local_applicant_folder, exist_ok=True)
        
            if pdf_data:
                local_file_path = os.path.join(local_applicant_folder, new_file_name)
                with open(local_file_path, "wb") as pdf_file:
                    pdf_file.write(base64.b64decode(pdf_data))
                print(f"Archivo PDF guardado como {local_file_path}")
            else:
                print("No se encontró contenido en la respuesta.")
        else:
            print(f"Error en la solicitud: {response.status_code}, {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error HTTP
    except Exception as e:
        print(f"Error procesando la respuesta: {e}")
        return pd.DataFrame()


