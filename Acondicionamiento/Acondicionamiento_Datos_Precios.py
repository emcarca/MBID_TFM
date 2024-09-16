import random
import string
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import json
import pandas as pd

""" DEFINICIÓN DE FUNCIONES AUXILIARES """
### PENDIENTE DE AJUSTAR PARA UNIDAD MEDIA QUE NO CARGA MW
# Definición de funciones
import sqlite3
import pandas as pd
from sqlalchemy import create_engine

def Generacion_Preprocesado_InsertBD_SQLALCHEMY(dataframeGeneracion):
  # Datos de conexión a POSTGRESQL
  usuario = 'admin'
  password = '08rFHGN0j1im68956jwW7yYf'
  servidor = 'formerly-top-toad-iad.a1.pgedge.io'  # o la dirección IP de tu servidor
  puerto = '5432'
  basedatos = 'emiliocardona_09mbid'

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


""" Código principal - Acondicionamiento de datos de generación de energía """
