import os
import boto3

def get_s3_client():
    return boto3.client(
        service_name="s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

def download_from_answers(answers, applicant_id, applicant_name, campus_code, stage, nivel, jornada, output_path, key_mapping, bucket, s3_client):
    for key, url_list in answers.items():
        for url in url_list:
            if not url:
                continue
            try:
                key_path = url.split("amazonaws.com/")[1]
                file_extension = os.path.splitext(url.split("/")[-1])[1]
                renamed_key = key_mapping.get(key, key)
                new_file_name = f"{renamed_key}_{applicant_name}_{applicant_id}{file_extension}"
                local_path = os.path.join(output_path, campus_code, stage, nivel, jornada, f"{applicant_name}_{applicant_id}")
                os.makedirs(local_path, exist_ok=True)

                print(f"Descargando {key} -> {local_path}")
                s3_client.download_file(bucket, key_path, os.path.join(local_path, new_file_name))
            except Exception as e:
                print(f"Error al procesar URL '{url}': {e}")
