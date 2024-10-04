""" DEFINICIÓN DE FUNCIONES AUXILIARES """
""" Funciones Globales """
# 1. Conexión con BD
# Se definen 2 posibles funciones para conexión a BD
# Se omiten los datos de conexión al tratarse de un entorno privado.
# 1.1 Con PSYCOPG2
def conexionBDPostgresSQL():
    conexion = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="",
        port="5432"
    )
    return conexion

# 2. Funcion para obtener listado de estaciones desde BBDD
def climatologiaObtenerListadoEstaciones():
    # Crea un cursor para realizar operaciones en la base de datos
    conexion = conexionBDPostgresSQL()
    cursor = conexion.cursor()

    # Define tu consulta SQL
    consulta_sql = """
    SELECT codigoEstacionclimatica
    FROM tfm.estacionclimatologiaprovincia
    ORDER BY provincia ASC;
    """

    # Ejecuta la consulta
    cursor.execute(consulta_sql)

    # Recupera los resultados
    resultados = cursor.fetchall()

    # Cierra el cursor y la conexión
    cursor.close()
    conexion.close()

    # Convierte los resultados a una lista de códigos de estación climática
    lista_codigos = [fila[0] for fila in resultados]
    print(lista_codigos)
    return lista_codigos
    
    
# 3. Descargar datos Climatológicos API - OPEN DATA
def descargarDatosClimatologiaOpenData(listadoEstaciones):
    # Código MAIN
    # Parámetros de la solicitud
    urlBaseApiOpenData = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/'

    anyoInicio = 2011
    anyoFin=2024

    for estacionClimatica in listadoEstaciones:
        # Iteración desde AnyoInicio a AnyoFin
        for anyo in range(anyoInicio, anyoFin + 1):
            # 1 semestre
            print("1er Semestre: ", anyo)
            start_date = datetime(anyo, 1, 1)
            end_date = datetime(anyo, 6, 30, 23, 59)

            # Crear el diccionario de parámetros
            parametros = {
                "fechaini": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "fechafin": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "estacion": estacionClimatica
            }

            # Completar URL consulta:             
            urlDatosClimatologicos = urlBaseApiOpenData+'fechaini/'+parametros["fechaini"]+'UTC/fechafin/'+parametros["fechafin"]+'UTC/estacion/'+parametros["estacion"]
            # print("urlDatosClimatologicos: ", urlDatosClimatologicos)
            
            # Invocar API OpenData
            datos = invocacionApiOpenData(urlDatosClimatologicos)
            # Insertar BBDD
            ClimatologiaProvincias_JSONData_InsertBD(datos)
            insertarConsultaAPI("Climatología", urlDatosClimatologicos , json.dumps(datos))

            # 2 semestre
            print("2o Semestre: ", anyo)
            start_date = datetime(anyo, 7, 1)
            end_date = datetime(anyo, 12, 31, 23, 59)

            # Crear el diccionario de parámetros
            parametros = {
                "fechaini": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "fechafin": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "estacion": estacionClimatica
            }

            # Completar URL consulta: 
            urlDatosClimatologicos = urlBaseApiOpenData+'fechaini/'+parametros["fechaini"]+'UTC/fechafin/'+parametros["fechafin"]+'UTC/estacion/'+parametros["estacion"]
            print("urlDatosClimatologicos: ", urlDatosClimatologicos)
            
            # Invocar API OpenData
            datos = invocacionApiOpenData(urlDatosClimatologicos)
            # Insertar BBDD
            ClimatologiaProvincias_JSONData_InsertBD(datos)
            insertarConsultaAPI("Climatología", urlDatosClimatologicos , json.dumps(datos))
            

""" Código principal MAIN """
# AEMET OpenData
# Paso 1. Obtener listado de estaciones climatológicas
import requests
import pandas as pd

# Parámetros de la solicitud
api_url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/todas'
headers = {
    'accept': 'application/json',
    'api_key': '' # SE OMITE APY_KEY al ser nominal
}

# Realizar la solicitud GET
response = requests.get(api_url, headers=headers)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    data = response.json()
    if data.get('estado') == 200:
        # Descargar los datos desde la URL proporcionada
        data_url = data.get('datos')
        datos_response = requests.get(data_url)
        if datos_response.status_code == 200:
            datos = datos_response.json()

            # Verificar que los datos están en el formato esperado
            if isinstance(datos, list):
                # Crear una lista para almacenar los datos extraídos
                datos_estaciones = []

                # Extraer los campos deseados
                for entry in datos:
                    idestacion = entry.get('idema')
                    longitud = entry.get('lon')
                    latitud = entry.get('lat')
                    ubi = entry.get('ubi')
                    if idestacion is not None and ubi is not None:
                        datos_estaciones.append({
                            'idestacion': idestacion,
                            'longitud': longitud,
                            'latitud': latitud,
                            'ubi': ubi
                        })

                # Crear el DataFrame
                estacionesClimatologicas = pd.DataFrame(datos_estaciones)
                print(estacionesClimatologicas)
                estacionesClimatologicas.to_csv('estaciones.csv', index=False)
            else:
                print("Los datos no están en el formato esperado (deberían ser una lista).")
        else:
            print(f"Error al descargar los datos: {datos_response.status_code}")
    else:
        print(f"Error en la respuesta de la API: {data.get('descripcion')}")
else:
    print(f"Error en la solicitud: {response.status_code}")

# Se lee manualmente el CSV y se insertan 52 registros de staciones en base de datos.

# Paso 2: Obtener datos climatología
listadoEstaciones = climatologiaObtenerListadoEstaciones()
descargarDatosClimatologiaOpenData(listadoEstaciones)
