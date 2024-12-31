import requests
import base64
import os
import pandas as pd


# Configura el Bearer Token para la autenticación
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
API_URL = "https://api.tether.education/crm/download"

# Función para realizar la solicitud y descargar el contrato
def download_admission_form(campus_code):
    query = """
    query DownloadAdmissionDigitalEnrollmentFormBasic {
        downloadAdmissionDigitalEnrollmentFormBasic(
            filter: { 
                contentType: EXCEL 
                contextTypes: "admission-completed"
                processIDs: ["6", "7", "8", "9", "100"]
            }
        ) {
            filename
            contentSource
            data
        }
    }
    """

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

        # Procesar la respuesta
        data = response.json()
        result = data.get("data", {}).get("downloadAdmissionDigitalEnrollmentFormBasic", {})

        if result:
            # Obtener el contenido del archivo
            file_content = result.get("data", "")

            # Decodificar el contenido base64
            decoded_content = base64.b64decode(file_content)

            # Leer el contenido como un DataFrame usando pandas
            df = pd.read_excel(decoded_content)

            print(f"Datos cargados en un DataFrame con {len(df)} filas.")
            return df
        else:
            print("No se encontró información en la respuesta.")
            return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error lógico

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error HTTP
    except Exception as e:
        print(f"Error procesando la respuesta: {e}")
        return pd.DataFrame()