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
# Se omiten los datos de conexión al tratarse de un entorno privado.
## Se definen 2 posibles funciones para conexión a BD
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


# 1.2 Con SQLALCHEMY(dataframeGeneracion):
def conexionBD_SQLALCHEMY():
  # Datos de conexión a POSTGRESQL
  usuario = ''
  password = ''
  servidor = ''  
  puerto = '5432'
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
    
    
""" FUNCIONES DEMANDA DE ENERGÍA """
# 1. Función para INSERTAR en BD (Demanda)
def Demanda_JSONData_InsertBD(data):
  # Obtener los campos que interesan
  datosDemanda = []

  for item in data["included"]:
    for value_item in item["attributes"]["values"]:
      itemDemanda = {
              "valorDemanda": value_item["value"],
              "fechaDemanda": value_item["datetime"],
          }
      datosDemanda.append(itemDemanda)

  #print(datosDemanda)
  sqlScript = []

  for sqlItem in datosDemanda:
      sentenciaSQL = f"""INSERT INTO tfm.energiademanda
      (fechaDemanda, valorDemanda, unidadMedida)
      VALUES ('{sqlItem['fechaDemanda']}', '{sqlItem['valorDemanda']}', 'MWh');"""

      sqlScript.append(sentenciaSQL)

  # Separar instrucciones por línea
  insertSQLScript = "\n".join(sqlScript)

  #print(insertSQLScript)
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
  
    
""" Código principal - Descarga de demanda de energía """
# Descargar histórico de demanda
import requests
from datetime import datetime

# URL de la API
urlDemanda = "https://apidatos.ree.es/es/datos/demanda/evolucion"

# Desde el año 2010 realiar invocación y almacenar datos si no hay error:
# Parámetros Año Inicio y Fin
anyoInicio = 2015
anyoFin = datetime.now().year

# Lista para almacenar los parámetros
parametros_consulta = []

# Iterar sobre los años desde el año de inicio hasta el año actual
for anyo in range(anyoInicio, anyoFin + 1):
  start_date = datetime(anyo, 7, 12)
  end_date = datetime(anyo, 7, 31, 23, 59)

  # Crear el diccionario de parámetros
  parametros = {
      "start_date": start_date.strftime("%Y-%m-%dT%H:%M"),
      "end_date": end_date.strftime("%Y-%m-%dT%H:%M"),
      "time_trunc": "day"
  }
  print(parametros)
  # Realizar la solicitud GET a la API
  response = requests.get(urlDemanda, params=parametros)

  print(response.url.replace("%3A", ":"))

  # Comprobar si la solicitud fue exitosa (código de estado 200)
  if response.status_code == 200:
    # Obtener los datos JSON
    jsonRespuesta = response.json()
    # Insertar en BD:
    Demanda_JSONData_InsertBD(jsonRespuesta)
    jsonRespuestaString = json.dumps(jsonRespuesta)
  else:
      print("Error al obtener los datos de la API:", response.status_code, " - ") #, json.dumps(jsonRespuesta))
      jsonRespuestaString = "Error: "+str(response.status_code)
  print(jsonRespuestaString)
  insertarConsultaAPI("Demanda", response.url.replace("%3A", ":") , jsonRespuestaString)
