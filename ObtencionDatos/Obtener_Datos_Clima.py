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
    conexion = psycopg2.connect(
        dbname="emiliocardona_09mbid",
        user="admin",
        password="08rFHGN0j1im68956jwW7yYf",
        host="formerly-top-toad-iad.a1.pgedge.io",
        port="5432"
    )
    return conexion


# 1.2 Con SQLALCHEMY(dataframeGeneracion):
def conexionBD_SQLALCHEMY():
  # Datos de conexión a POSTGRESQL
  usuario = 'admin'
  password = '08rFHGN0j1im68956jwW7yYf'
  servidor = 'formerly-top-toad-iad.a1.pgedge.io'  
  puerto = '5432'
  basedatos = 'emiliocardona_09mbid'
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

  
  
  
""" Código principal - Descarga de demanda de energía """
