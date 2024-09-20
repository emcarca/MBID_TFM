import random
import string
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import json
import pandas as pd

""" DEFINICIÓN DE FUNCIONES AUXILIARES """
""" Funciones Globales """
# 1. Conexión con BD
## Se definen 2 posibles funciones para conexión a BD
# 1.1 Con PSYCOPG2
def conexionBDPostgresSQL():
    # Se omiten los datos de conexión al tratarse de un entorno privado.
    conexion = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="",
        port=""
    )
    return conexion


# 1.2 Con SQLALCHEMY(dataframeGeneracion):
def conexionBD_SQLALCHEMY():
  # Datos de conexión a POSTGRESQL:  Se omiten los datos de conexión al tratarse de un entorno privado.
  usuario = ''
  password = ''
  servidor = ''  
  puerto = ''
  basedatos = ''
  # URL de conexión
  urlConexion = f'postgresql+psycopg2://{usuario}:{password}@{servidor}:{puerto}/{basedatos}'
  # Motor conexión Postgresql
  engine = create_engine(urlConexion)
  return engine


# 2. Insertar ConsultaAPI
""" Función para registrar Consulta + Respuesta y almacenar esta información
en bruto por si se precisa consultarla en el futuro """
def insertarConsultaAPI(tipoAPI, urlConsulta, jsonRespuesta):
    connection = conexionBDPostgresSQL()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        insert_query = sql.SQL("""INSERT INTO tfm.consultas_api (tipoApi, urlConsulta, jsonRespuesta) VALUES (%s, %s, %s) """)
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
      print(f"Comando ejecutado con éxito: {comandoSQL[0:50]}")
  except Exception as error:
      print(f"Error al ejecutar el comando: {error}")
  finally:
      if connection:
          cursor.close()
          connection.close()

# 4. Función para ejecutar cualquier comando SQL (SELECT)
def ejecutarComandoSQLSelect(comandoSQL):
  connection = conexionBDPostgresSQL()
  resultados = []
  if connection is None:
      return
  try:
      cursor = connection.cursor()
      cursor.execute(comandoSQL)
      # Obtener todos los resultados de la consulta
      resultados = cursor.fetchall()
      connection.commit()
      print(f"Comando ejecutado con éxito: {comandoSQL[0:50]}")
  except Exception as error:
      print(f"Error al ejecutar el comando: \n{error}")
  finally:
    if connection:
        cursor.close()
        connection.close()
    return resultados
    
    
""" FUNCIONES GENERACIÓN DE ENERGÍA """
# 1. INSERTAR en BD Datos JSON (Generación)
def Generacion_JSONData_InsertBD(data):
  # Del JSON se filtran los campos que interesan
  datosGeneracion = []
  for item in data["included"]:
      for value_item in item["attributes"]["values"]:
        fechaDato = value_item["datetime"][:10]
        dato = {
            "tipoenergiageneracion": item["attributes"]["title"],
            "tipoGeneracionRenovable": item["attributes"]["type"],
            "fechaGeneracion": datetime.strptime(fechaDato, '%Y-%m-%d'),
            "valorGeneracion": value_item["value"],
            "porcentajeGeneracion": value_item["percentage"]
        }
        datosGeneracion.append(dato)

  # Preparación del script SQL
  sqlScript = []
  for sqlItem in datosGeneracion:
      sentenciaSQL = f"""INSERT INTO tfm.energiageneracion (tipoenergiageneracion, tipogeneracionrenovable,
      fechaGeneracion, valorGeneracion, unidadMedida, porcentajeGeneracion)
      VALUES ('{sqlItem['tipoenergiageneracion']}', '{sqlItem['tipoGeneracionRenovable']}',
        '{sqlItem['fechaGeneracion']}', {sqlItem['valorGeneracion']}, 'MWh', '{sqlItem['porcentajeGeneracion']}');"""
      sqlScript.append(sentenciaSQL)

  # Unir todas las sentencias SQL en una variable
  insertSQLScript = "\n".join(sqlScript)
  
  print("Insertar registros - ")
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
  print("Registros insertados - ")
  
  
  
""" Código principal - Descarga de generación de energía """
# Descargar histórico de generación
import requests
from datetime import datetime

# URL de la API
urlAPIGeneracion = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"

# Desde el año 2011 realiar invocación y almacenar datos si no hay error:
# Parámetros Año Inicio y Fin
anyoInicio = 2011
anyoFin = datetime.now().year

# Consulta ANUAL en el rango de años establecido
for anyo in range(anyoInicio, anyoFin + 1):
  start_date = datetime(anyo, 1, 1)
  end_date = datetime(anyo, 12, 31, 23, 59)

  # Diccionario con los parámetros requeridos en la petición a la API
  parametros = {
      "start_date": start_date.strftime("%Y-%m-%dT%H:%M"),
      "end_date": end_date.strftime("%Y-%m-%dT%H:%M"),
      "time_trunc": "day"
  }
  # Realizar la solicitud GET a la API
  response = requests.get(urlAPIGeneracion, params=parametros)
  print(response.url.replace("%3A", ":"))

  # Si estado = 200 (Respuesta OK)
  if response.status_code == 200:
    # Obtener los datos JSON
    jsonRespuesta = response.json()
    # Insertar en BD:
    Generacion_JSONData_InsertBD(jsonRespuesta)
    jsonRespuestaString = json.dumps(jsonRespuesta)
  else:
      print("Error al obtener los datos de la API:", response.status_code, " - ")
      jsonRespuestaString = "Error: "+str(response.status_code)
  insertarConsultaAPI("Generación", response.url.replace("%3A", ":") , jsonRespuestaString)
