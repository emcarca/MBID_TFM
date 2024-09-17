import requests
import random
import string
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import json
from decimal import Decimal, InvalidOperation

# 1. Conexión con BD
def conexionBDPostgresSQL():
    connection = psycopg2.connect(
        dbname="emiliocardona_09mbid",
        user="admin",
        password="08rFHGN0j1im68956jwW7yYf",
        host="formerly-top-toad-iad.a1.pgedge.io",
        port="5432"
    )
    return connection

# 2. Insertar ConsultaAPI
def insertarConsultaAPI(tipoAPI, urlConsulta, jsonRespuesta):
    connection = conexionBDPostgresSQL()    
    if connection is None:
        return
    try:
        cursor = connection.cursor()

        insert_query = sql.SQL("""INSERT INTO tfm.consultas_api (tipoApi, urlConsulta, jsonRespuesta)
            VALUES (%s, %s, %s) """)

        cursor.execute(insert_query, (tipoAPI, urlConsulta, jsonRespuesta))

        connection.commit()
        print("Inserción realizada con éxito")
    except Exception as error:
        print(f"Error al insertar en la base de datos: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

# 3. Función para ejecutar cualquier comando SQL
def ejecutarComandoSQL(comandoSQL):
  connection = conexionBDPostgresSQL()
  if connection is None:
      return
  try:
      cursor = connection.cursor()
      cursor.execute(comandoSQL)
      connection.commit()
      print("Comando ejecutado con éxito.")
  except Exception as error:
      print(f"Error al ejecutar el comando: {error}")
  finally:
      if connection:
          cursor.close()
          connection.close()

# 4. Función invocación OPENDATA          
def invocacionApiOpenData(urlServicio):
  headers = {
    'accept': 'application/json',
    'api_key': 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJlbWNhcmNhQGdtYWlsLmNvbSIsImp0aSI6IjY5ZTU2OWYxLTgwMmMtNDEwYy1hM2FlLWE3ODFjNTdjNDdlYSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzA4OTY3MTU1LCJ1c2VySWQiOiI2OWU1NjlmMS04MDJjLTQxMGMtYTNhZS1hNzgxYzU3YzQ3ZWEiLCJyb2xlIjoiIn0.s8eER60NS6xvqvHEoRuFi0RWJzArV9U_0rGQKG58QXY'
  }

  # Realizar la solicitud GET
  response = requests.get(urlServicio, headers=headers)
  
  # Verificar si la solicitud fue exitosa
  if response.status_code == 200:
    data = response.json()
    if data.get('estado') == 200:
        # Descargar los datos desde la URL proporcionada
        data_url = data.get('datos')
        datos_response = requests.get(data_url)
        if datos_response.status_code == 200:
            datos = datos_response.json()
            return datos
        else:
            print(f"Error al descargar los datos: {datos_response.status_code}")
    else:
        print(f"Error en la respuesta de la API: {data.get('descripcion')}")
  else:
    print(f"Error en la solicitud: {response.status_code}")
    return None


# 5.0 Función para convertir a Decimal los valores numéricos
def convertirDecimal(valor):
    if valor is not None:
        try:
            valor = Decimal(valor.replace(",","."))
        except InvalidOperation:
            valor = None  # O maneja el error de otra manera que tenga sentido para tu caso
    else:
        valor = None
    
    return valor
 

# 5. Insertar datos climatologia en BD
def ClimatologiaProvincias_JSONData_InsertBD(datosJson):
    # Obtener los campos que interesan
    datosClimatologiaProvincias = []

    # Verificar que los datos están en el formato esperado
    if isinstance(datosJson, list):
      print(len(datosJson))
      #print(datosJson)
      for item in datosJson:
          #print(item)
          # .get() no requiere comprobar si existe el campo previamente
          # item[sol] fallaba 
          datoClimaEstacionFecha = {
            "indicativoEstacion" : item.get('indicativo', None),
            "descripcionEstacion" : item.get('nombre', None),
            "fecha" : item.get('fecha', None),
            "provincia" : item.get('provincia', None),
            "temperaturaMedia" : convertirDecimal(item.get('tmed', None)),
            "temperaturaMinima" : convertirDecimal(item.get('tmin', None)),
            "temperaturaMaxima" : convertirDecimal(item.get('tmax', None)),
            "precipitaciones" : convertirDecimal(item.get('prec', None)),
            "vientoVelMedia" : convertirDecimal(item.get('velmedia', None)),
            "vientoRacha" : convertirDecimal(item.get('racha', None)),
            "sol" : convertirDecimal(item.get('sol', None)),
            "humedadrelativamedia" : convertirDecimal(item.get('hrMedia', None)),
            "humedadrelativaminima" : convertirDecimal(item.get('hrMin', None)),
            "humedadrelativamaxima" : convertirDecimal(item.get('hrMax', None)),
            "presionmaxima" : convertirDecimal(item.get('presMax', None)),
            "presionminima" : convertirDecimal(item.get('presMin', None))            
            }
                               
          datosClimatologiaProvincias.append(datoClimaEstacionFecha)
    print(len(datosClimatologiaProvincias))
    
    sqlScript = []

    for sqlItem in datosClimatologiaProvincias:
        sentenciaSQL = f"""INSERT INTO tfm.climatologiaprovincias 
        (indicativoEstacion, descripcionEstacion,fecha, provincia, 
         temperaturaMedia,temperaturaMinima,temperaturaMaxima, 
         precipitaciones,vientoVelMedia,vientoRacha,sol,
         humedadrelativamedia,humedadrelativaminima,humedadrelativamaxima,
         presionmaxima,presionminima)  
        VALUES ('{sqlItem['indicativoEstacion']}', 
                '{sqlItem['descripcionEstacion']}', 
                '{sqlItem['fecha']}', 
                '{sqlItem['provincia']}', 
                {sqlItem['temperaturaMedia']}, 
                {sqlItem['temperaturaMinima']},
                {sqlItem['temperaturaMaxima']}, 
                {sqlItem['precipitaciones']},
                {sqlItem['vientoVelMedia']}, 
                {sqlItem['vientoRacha']}, 
                {sqlItem['sol']},
                {sqlItem['humedadrelativamedia']}, 
                {sqlItem['humedadrelativaminima']},
                {sqlItem['humedadrelativamaxima']}, 
                {sqlItem['presionmaxima']}, 
                {sqlItem['presionminima']});"""
        sqlScript.append(sentenciaSQL)

    # Joining all SQL statements into a single script
    insertSQLScript = "\n".join(sqlScript).replace("None", "NULL")
    #print(insertSQLScript)
    print("Insertar registros - ", len(sqlScript))
    conexionBD = conexionBDPostgresSQL()
    # Crear un cursor
    cursor = conexionBD.cursor()

    # Ejecutar el script SQL
    cursor.execute(insertSQLScript)

    # Confirmar los cambios
    conexionBD.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    conexionBD.close()
    print("Registros insertados - ", len(sqlScript))





# 6. Descargar datos Climatológicos API - OPEN DATA
def descargarDatosClimatologiaOpenData(listadoEstaciones):
    # Código MAIN
    # Parámetros de la solicitud
    urlBaseApiOpenData = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/'
    
    anyoInicio = 2020
    anyoFin=2024
    
    for estacionClimatica in listadoEstaciones:
        #print(estacionClimatica)
        
        # Iterar sobre los años desde el año de inicio hasta el año actual
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
            
            # Completar URL consulta: Ejemplo parametros > 'fechaini/2019-01-01T00%3A00%3A00UTC/fechafin/2019-01-02T00%3A00%3A00UTC/estacion/8414A,8416X,8416Y'
            urlDatosClimatologicos = urlBaseApiOpenData+'fechaini/'+parametros["fechaini"]+'UTC/fechafin/'+parametros["fechafin"]+'UTC/estacion/'+parametros["estacion"]
            print("urlDatosClimatologicos: ", urlDatosClimatologicos)
            # Invocar API OpenData
            datos = invocacionApiOpenData(urlDatosClimatologicos)
            # Insertar BBDD
            insertarConsultaAPI("Clima", urlDatosClimatologicos , json.dumps(datos))
            if datos is not None and len(datos) > 0:            
                ClimatologiaProvincias_JSONData_InsertBD(datos)
            
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
            
            # Completar URL consulta: Ejemplo parametros > 'fechaini/2019-01-01T00%3A00%3A00UTC/fechafin/2019-01-02T00%3A00%3A00UTC/estacion/8414A,8416X,8416Y'
            urlDatosClimatologicos = urlBaseApiOpenData+'fechaini/'+parametros["fechaini"]+'UTC/fechafin/'+parametros["fechafin"]+'UTC/estacion/'+parametros["estacion"]
            print("urlDatosClimatologicos: ", urlDatosClimatologicos)
            # Invocar API OpenData
            datos = invocacionApiOpenData(urlDatosClimatologicos)
            # Insertar BBDD
            insertarConsultaAPI("Clima", urlDatosClimatologicos , json.dumps(datos))
            if datos is not None and len(datos) > 0:            
                ClimatologiaProvincias_JSONData_InsertBD(datos)
            #response = requests.get(urlAPIGeneracion, params=parametros)
            #print(response.url.replace("%3A", ":"))"""
            
        print("Sleep")
        time.sleep(5)
        print("Go")

"""
 # 6. Descargar datos Climatológicos API - OPEN DATA
def descargarDatosClimatologiaOpenData_BK():
   # Código MAIN
    # Parámetros de la solicitud
    urlBaseApiOpenData = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/'
    
    anyoInicio = 2019
    anyoFin=2019
    # Iterar sobre los años desde el año de inicio hasta el año actual
    for anyo in range(anyoInicio, anyoFin + 1):
        # 1 semestre
        print("1er Semestre: ", anyo)
        start_date = datetime(anyo, 1, 1)
        end_date = datetime(anyo, 6, 30, 23, 59)
          
        # Crear el diccionario de parámetros
        parametros = {
            "fechaini": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "fechafin": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "estacion": "8414A,8416X,8416Y"
        }
        
        # Completar URL consulta: Ejemplo parametros > 'fechaini/2019-01-01T00%3A00%3A00UTC/fechafin/2019-01-02T00%3A00%3A00UTC/estacion/8414A,8416X,8416Y'
        urlDatosClimatologicos = urlBaseApiOpenData+'fechaini/'+parametros["fechaini"]+'UTC/fechafin/'+parametros["fechafin"]+'UTC/estacion/'+parametros["estacion"]
        print("urlDatosClimatologicos: ", urlDatosClimatologicos)
        # Invocar API OpenData
        datos = invocacionApiOpenData(urlDatosClimatologicos)
        # Insertar BBDD
        #ClimatologiaProvincias_JSONData_InsertBD(datos)
        #insertarConsultaAPI("Climatología", urlDatosClimatologicos , json.dumps(datos))
        
        # 2 semestre
        print("2o Semestre: ", anyo)
        start_date = datetime(anyo, 7, 1)
        end_date = datetime(anyo, 12, 31, 23, 59)
          
        # Crear el diccionario de parámetros
        parametros = {
            "fechaini": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "fechafin": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "estacion": "8414A,8416X,8416Y"
        }
        
        # Completar URL consulta: Ejemplo parametros > 'fechaini/2019-01-01T00%3A00%3A00UTC/fechafin/2019-01-02T00%3A00%3A00UTC/estacion/8414A,8416X,8416Y'
        urlDatosClimatologicos = urlBaseApiOpenData+'fechaini/'+parametros["fechaini"]+'UTC/fechafin/'+parametros["fechafin"]+'UTC/estacion/'+parametros["estacion"]
        print("urlDatosClimatologicos: ", urlDatosClimatologicos)
        # Invocar API OpenData
        datos = invocacionApiOpenData(urlDatosClimatologicos)
        # Insertar BBDD
        ClimatologiaProvincias_JSONData_InsertBD(datos)
        #insertarConsultaAPI("Climatología", urlDatosClimatologicos , json.dumps(datos))
        #response = requests.get(urlAPIGeneracion, params=parametros)
        #print(response.url.replace("%3A", ":"))
"""

# 7. Funcion para obtener listado de estaciones desde BBDD
def climatologiaObtenerListadoEstaciones():
    # Conexión a BBDD y cursor
    conexion = conexionBDPostgresSQL()
    cursor = conexion.cursor()
    
    # Comando SQL
    comandoSQL = """
    SELECT codigoEstacionclimatica 
    FROM tfm.estacionclimatologiaprovincia 
    ORDER BY provincia ASC;
    """
    
    # Ejecuta la consulta
    cursor.execute(comandoSQL)
    
    # Resultados consulta
    resultados = cursor.fetchall()
    
    # Cerrar cursor y conexión
    cursor.close()
    conexion.close()
    
    # Convierte los resultados a una lista de códigos de estación climática
    listadoCodigoEstaciones = [fila[0] for fila in resultados]
    #print(listadoCodigoEstaciones)
    return listadoCodigoEstaciones

import time


# MAIN
# Llama a la función y muestra los resultados
#listadoEstaciones = ['1387', '8175', '8019', '6325O', '1212E', '4452', '0076', '2331', '1109X', '8500A', '5000C', '4121', '8096', '3469A', '5973', '5402', '1014A', '0367', '5514Z', '3168D', '4642E', '9898', 'B278', '5270B', '9170', 'C649I', '2661', '9771C', '1505', '3129', '6000A', '7178I', '6155A', '9263D', '1690A', '2400E', '1484C', '2867', 'C449C', '2465', '5783', '2030', '0042Y', '8368U', '3260B', '8414A', '2422', '1082', '2614', '9434', '9091R', '2444']
listadoEstaciones = climatologiaObtenerListadoEstaciones()
print("Listado: ", listadoEstaciones)
print(listadoEstaciones)
descargarDatosClimatologiaOpenData(listadoEstaciones) 

#codigos_estacion_climatica = obtener_codigos_estacion_climatica()
#print(codigos_estacion_climatica)
#descargarDatosClimatologiaOpenData()
#ClimatologiaProvincias_JSONData_InsertBD(datos)
#climatologiaObtenerListadoEstaciones()



