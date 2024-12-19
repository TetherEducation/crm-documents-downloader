import pandas as pd

def load_and_process_excel(input_path):
    df = pd.read_excel(input_path)
    df['nombre_estudiante'] = df[['Primer Nombre Alumno', 'Segundo Nombre Alumno', 
                                  'Primer Apellido Alumno', 'Segundo Apellido Alumno']].apply(
        lambda row: " ".join(row.dropna().str.strip()), axis=1
    )
    df = df.rename(columns={'Id': 'admission_id', 'Identificación Alumno': 'rut'})
    df['admission_id'] = df['admission_id'].astype(str)
    return df[['nombre_estudiante', 'rut', 'admission_id', 'Nivel', 'Jornada']]

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

def check_stage_empty(df, carpeta):
    if df["stage"].isna().any():
        alert_message = f"Alerta: Observaciones con stage vacío en la carpeta '{carpeta}'\n"
        alert_message += df[df["stage"].isna()].to_string(index=False)
        return alert_message
    return None
