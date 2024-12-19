# crm-documents-downloader

**input_dir**: Es la dirección donde deben estar los archivos de excel descargados del crm con el formato: 
   
   campus_code   
    > Ficha matrícula digital simple.xlsx

Al decargarlo de crm siempre viene con ese nombre así que basta con crear una carpeta para cada campus_code y guardar adentro la ficha descargada.

**output_dir**: Es la dirección donde se van a guardar los archivos descargados de s3. Se guarda un archivo por stage, nivel, jornada y nombre de cada niño.

**carpetas**: En carpetas se debe poner el listado de campus_code que se quiere procesar. Ejemplo: carpetas = ["931800001", "1471700001"]

**template_types**: Son todos los tipos de templates que guardan algún archivo en s3. Si se agregan nuevos hay que hacer update de esto.

**key_mapping**: Es un mapeo de los key con los que se guardan los archivos en s3 a un string más comprensible para los colegios.

**alertas.txt**: Es un archivo de texto que guarda las alertas que se encontraron en la ejecución como nombres de nivel diferentes que no se ajustan a los stage pre-seteados o estudiantes descargados de crm que no se encontraron en mongo.
