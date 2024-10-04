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
    
""" FUNCIONES LECTURA Y CARGA - Precio de energía"""
import pandas as pd
import re # Expresiones regulares

# Función para leer excel e insertarlo en base de datos
def leerFicheroExcelPrecioMercado(rutaArchivo):
    # Cargar y leer el archivo excel
    dataframePrecios = pd.read_excel(rutaArchivo)

    # Renombrar columnas con nombre numérico (1, 2, 3, ...) a HoraX
    # Se usa expresión regular
    columnasRenombrar = {col: f"hora{col}"
                         if re.match(r'^\d+$', str(col)) # Expresión regular para números
                         else col
                         for col in dataframePrecios.columns}

    # Se renombra también las columnas "Fecha/hora" y "Media diaria"
    columnasRenombrar["Fecha/Hora"]='fecha'
    columnasRenombrar["Media diaria"]='preciomedio'
    dataframePrecios.rename(columns=columnasRenombrar, inplace=True)

    return dataframePrecios

def PrecioEnergia_XLSX_InsertBD(dataframePrecios):
  sqlScript = []
  # Recorrer el dataframePrecios y preparar sentencia INSERT
  for indice, registro in dataframePrecios.iterrows():
      sentenciaSQL = f"""INSERT INTO tfm.energiapreciomercado (fecha, hora1, hora2, hora3, hora4, hora5, hora6, hora7,
      hora8, hora9, hora10, hora11, hora12, hora13, hora14, hora15, hora16, hora17, hora18,
      hora19, hora20, hora21, hora22, hora23, hora24, preciomedio) VALUES (
      '{registro['fecha']}',
      {registro['hora1']},
      {registro['hora2']},
      {registro['hora3']},
      {registro['hora4']},
      {registro['hora5']},
      {registro['hora6']},
      {registro['hora7']},
      {registro['hora8']},
      {registro['hora9']},
      {registro['hora10']},
      {registro['hora11']},
      {registro['hora12']},
      {registro['hora13']},
      {registro['hora14']},
      {registro['hora15']},
      {registro['hora16']},
      {registro['hora17']},
      {registro['hora18']},
      {registro['hora19']},
      {registro['hora20']},
      {registro['hora21']},
      {registro['hora22']},
      {registro['hora23']},
      {registro['hora24']},
      {registro['preciomedio']});"""

      sqlScript.append(sentenciaSQL)

  # Unir todas las sentencias en una única.
  insertSQLScript = "\n".join(sqlScript).replace("nan", "NULL")
  print("Insertar registros - Precio Mercado")
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
  print("Registros insertados -  Precio Mercado")
 
  
""" Código principal - Precio de energía """
# Ruta al archivo excel
archivoPrecios = 'OMIE_Precios_historico.xlsx'
# Paso 1: Lectura del archivo excel
dataframePrecios = leerFicheroExcelPrecioMercado(archivoPrecios)

# Paso 2: Insertar en base de datos
PrecioEnergia_XLSX_InsertBD(dataframePrecios)

# Paso 3: Verificar estructura del contenido.
dataframePrecios.head()
