### PENDIENTE DE AJUSTAR PARA UNIDAD MEDIA QUE NO CARGA MW
# Definición de funciones
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql
import json 

# 1. Conexión con BD
# 1.1 Versión 1: Usando librería psycopg2
def conexionBDPostgresSQL():
    connection = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="",
        port="5432"
    )
    return connection

# 1.2 Insertar datos Generación
def Generacion_Preprocesado_InsertBD_SQLALCHEMY(dataframeGeneracion):
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

  # Tabla y Esquema de BD
  esquemaBD = 'tfm'
  tabla = 'preproc_energiageneracion'

  # Insertar en BBDD desde un Dataframe
  dataframeGeneracion.to_sql(tabla, engine, schema=esquemaBD, if_exists='append', index=False)


# 2. # Obtener datos generación desde BBDD y generar Dataframe y CSV
def Preprocesamiento_GeneracionEnergia_ToDataframe():
  # Consultar diferentes tipos de energía
  comandoSQL = """
    SELECT fechageneracion, tipoenergiageneracion, valorgeneracion, unidadMedida
    FROM tfm.energiageneracion g
    WHERE g.fechageneracion BETWEEN '2011-01-01' AND '2024-07-31'
  """
  registrosGeneracion = ejecutarComandoSQLSelect(comandoSQL)

  # Diccionario dónde insertar los datos
  datosGeneracion = {
      'fechaGeneracion': [],
      'Carbon_NR': [],
      'CicloCombinado_NR': [],
      'Cogeneracion_NR': [],
      'Eolica_R': [],
      'FuelGas_NR': [],
      'Hidroeolica_R': [],
      'Hidraulica_R': [],
      'MotorDiesel_NR': [],
      'Nuclear_NR': [],
      'OtrasRenovables_R': [],
      'SolarFotovoltaica_R': [],
      'SolarTermica_R': [],
      'TurbinaGas_NR': [],
      'TurbinaVapor_NR': [],
      'TurbinaBombeo_NR': [],
      'ResiduosNoRenov_NR': [],
      'ResiduosRenov_R': [],
      'UnidadMedida': [],
      'GeneracionTotal': []
  }

  # Diccionario para Mapear tipo de energía a nombre de columna
  conversionTiposGeneracionEnergia = {
    'Carbón': 'Carbon_NR',
    'Ciclo combinado': 'CicloCombinado_NR',
    'Cogeneración': 'Cogeneracion_NR',
    'Eólica': 'Eolica_R',
    'Fuel + Gas': 'FuelGas_NR',
    'Hidroeólica': 'Hidroeolica_R',
    'Hidráulica': 'Hidraulica_R',
    'Motores diésel': 'MotorDiesel_NR',
    'Nuclear': 'Nuclear_NR',
    'Otras renovables': 'OtrasRenovables_R',
    'Solar fotovoltaica': 'SolarFotovoltaica_R',
    'Solar térmica': 'SolarTermica_R',
    'Turbina de gas': 'TurbinaGas_NR',
    'Turbina de vapor': 'TurbinaVapor_NR',
    'Turbinación bombeo': 'TurbinaBombeo_NR',
    'Residuos renovables': 'ResiduosRenov_R',
    'Residuos no renovables': 'ResiduosNoRenov_NR',
    'Generación total' : 'GeneracionTotal'
  }

  # Procesar los resultados y agregar los datos al diccionario
  for registro in registrosGeneracion:
    fecha = registro[0]
    tipoGeneracion = registro[1]
    valor_generacion = registro[2]
    unidadMedida= registro[3]
    if fecha not in datosGeneracion['fechaGeneracion']:
        datosGeneracion['fechaGeneracion'].append(fecha)
        for key in datosGeneracion.keys():
            if key != 'fechaGeneracion' and key != 'UnidadMedida':
                datosGeneracion[key].append(0)  # Resto de columnas se inician en 0
            elif key == 'UnidadMedida':
                datosGeneracion[key].append('MWh')

    index = datosGeneracion['fechaGeneracion'].index(fecha)
    columna = conversionTiposGeneracionEnergia.get(tipoGeneracion, None)

    if columna:
        datosGeneracion[columna][index] = valor_generacion

  # Crear dataframe desde "Datos Generación"
  dataframeGeneracion = pd.DataFrame(datosGeneracion)

  # DataFrame a CSV
  dataframeGeneracion.to_csv('GeneracionFormateado.csv', index=False)

  # Insertar en BD
  Generacion_Preprocesado_InsertBD_SQLALCHEMY(dataframeGeneracion)

  
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


""" Código principal (MAIN) """
# Paso 1: Vaciar la tabla en la que se insertarán los registros acondicionados
ejecutarComandoSQL("TRUNCATE TABLE tfm.preproc_energiageneracion;")
# Paso 2: Ejecución de la función que acondiciona los datos.
Preprocesamiento_GeneracionEnergia_ToDataframe()
