import pandas as pd

def process_student_data(df):
    df['nombre_estudiante'] = df[['Primer Nombre Alumno', 'Segundo Nombre Alumno', 
                                  'Primer Apellido Alumno', 'Segundo Apellido Alumno']].apply(
        lambda row: " ".join(row.dropna().str.strip()), axis=1
    )
    df = df.rename(columns={'Id Aplicación': 'applicationId', 'Identificación Alumno': 'rut'})
    df['applicationId'] = df['applicationId'].astype(str)
    return df[['nombre_estudiante', 'rut', 'applicationId', 'Nivel', 'Jornada']]

def map_stage(df):
    nivel_map = {
        "PreKinder": "1. Parvularia",
        "Kinder": "1. Parvularia",
        "1ro Básico": "2. Básica",
        "2do Básico": "2. Básica",
        "3ro Básico": "2. Básica",
        "4to Básico": "2. Básica",
        "5to Básico": "2. Básica",
        "6to Básico": "2. Básica",
        "7mo Básico": "2. Básica",
        "8vo Básico": "2. Básica",
        "1ro Medio": "3. Media",
        "2do Medio": "3. Media",
        "3ro Medio": "3. Media",
        "4to Medio": "3. Media"
    }
    df["stage"] = df["Nivel"].map(nivel_map)
    return df

def check_stage_empty(df, campus_code):
    if df["stage"].isna().any():
        alert_message = f"Alerta: Observaciones con stage vacío en '{campus_code}'\n"
        alert_message += df[df["stage"].isna()].to_string(index=False)
        return alert_message
    return None
