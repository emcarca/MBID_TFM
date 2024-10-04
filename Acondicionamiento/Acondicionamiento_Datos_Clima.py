""" DEFINICIÓN DE FUNCIONES AUXILIARES """
""" Funciones Globales """ 
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import psycopg2
from psycopg2 import sql
import json 

# 1. Conexión con BD
# 1.1 Con PSYCOPG2
def conexionBDPostgresSQL():
    connection = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="",
        port="5432"
    )
    return connection

# 1.2 Con SQLALCHEMY(dataframeGeneracion):
def conexionBD_SQLALCHEMY():
  # Datos de conexión a POSTGRESQL
  usuario = ''
  password = ''
  servidor = ''
  puerto = '5432'
  basedatos = ''

  # Crear la URL de conexión
  urlConexion = f'postgresql+psycopg2://{usuario}:{password}@{servidor}:{puerto}/{basedatos}'
  # Motor conexión Postgresql
  engine = create_engine(urlConexion)

  return engine

# 2. Función para ejecutar cualquier comando SQL (SELECT)
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

      # Imprimir los resultados
      """for fila in resultados:
          print(fila)"""
      connection.commit()
      print(f"Comando ejecutado con éxito: {comandoSQL[0:50]}")
  except Exception as error:
      print(f"Error al ejecutar el comando: \n{error}")
  finally:
    if connection:
        cursor.close()
        connection.close()
    return resultados



""" Funciones - Acondicionamiento de datos Climatología """
# 1. Inserción en BBDD
def Preprocesado_Climatologia_InsertBD_SQLALCHEMY(dataframeClimatologia):
  motorConexion = conexionBD_SQLALCHEMY()

  # Tabla y Esquema de BD
  esquemaBD = 'tfm'
  tabla = 'preproc_climanacional'

  # Insertar en BBDD desde un Dataframe
  dataframeClimatologia.to_sql(tabla, motorConexion, schema=esquemaBD, if_exists='append', index=False)

# 2. Función que obtiene los datos en formato original y los procesa para acondicionarlos al formato final.
def Preprocesamiento_Climatologia_ToDataframe():
    # Comando SQL para descargar datos de la BBDD
    comandoSQL = """SELECT c.fecha, c.precipitaciones, c.sol, c.temperaturamedia,
        c.temperaturamaxima, c.temperaturaminima, c.vientoracha, c.vientovelmedia,
        c.humedadrelativamedia, c.humedadrelativamaxima, c.humedadrelativaminima,
        c.presionmaxima, c.presionminima
        FROM tfm.climatologiaprovincias c
        WHERE c.fecha between '2011-01-01' AND '2024-07-31'
        ORDER BY c.fecha ASC
    """
    registrosClimaProvincias = ejecutarComandoSQLSelect(comandoSQL)

    # Crear Dataframe a partir de los datos
    dataframeClima = pd.DataFrame(registrosClimaProvincias, columns=['fecha', 'precipitacionesAcumuladoNacional', 'solhorasMediaNacional',
        'temperaturaMediaNacional', 'temperaturaMaximaMediaNacional', 'temperaturaMinimaMediaNacional',
        'vientoRachaMediaNacional', 'vientoVelMediaNacional',
        'humedadRelativaMediaNacional', 'humedadRelativaMaxMediaNacional',
        'humedadRelativaMinMediaNacional', 'presionMaximaMediaNacional', 'presionMinimaMediaNacional'])

    # Fecha: tipo date
    dataframeClima['fecha'] = pd.to_datetime(dataframeClima['fecha'])

    # Agrupar los datos, por fecha, y aplicar una función agregada
    dataframeClima = dataframeClima.groupby('fecha').agg({
        'temperaturaMediaNacional': 'mean', # Media
        'temperaturaMaximaMediaNacional': 'mean',
        'temperaturaMinimaMediaNacional': 'mean',
        'precipitacionesAcumuladoNacional': 'sum', # Suma
        'solhorasMediaNacional': 'mean',
        'vientoRachaMediaNacional': 'mean',
        'vientoVelMediaNacional': 'mean',
        'humedadRelativaMediaNacional': 'mean',
        'humedadRelativaMaxMediaNacional': 'mean',
        'humedadRelativaMinMediaNacional': 'mean',
        'presionMaximaMediaNacional': 'mean',
        'presionMinimaMediaNacional': 'mean'
    }).reset_index()


    # DataFrame a CSV
    dataframeClima.to_csv('Preproc_ClimatologiaNacional.csv', index=False)

    # Insertar en BD
    Preprocesado_Climatologia_InsertBD_SQLALCHEMY(dataframeClima)
    
""" Código principal (MAIN) - """
# Paso 1: Vaciar el contenido de la tabla en la que se insertan los registros
ejecutarComandoSQL("TRUNCATE TABLE tfm.preproc_climanacional;")

# Paso 2: Ejecutar proceso de acondicionamiento.
resultados_climatologicos = Preprocesamiento_Climatologia_ToDataframe()

# Paso 3: Verificar datos tras la ejecución del proceso 
ejecutarComandoSQLSelect("SELECT COUNT(*) FROM tfm.preproc_climanacional")
ejecutarComandoSQLSelect("SELECT * FROM tfm.preproc_climanacional")
