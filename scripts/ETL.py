"""
ETAPA_1

Miembros del Equipo
Claudia Marín
Juan Sebastián Plazas
Stefan Giraldo

"""


#Importar las librerías
import pandas as pd
import yfinance as yf
import os
import logging
import pyodbc

#Carpetas para guardar los datos y los logs en local
log_dir = './logs'
data_dir = './data'

#Crear las carpetas si no existen
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)


#Log crea un archivo que guarda los loggins información del sistema
log_filename = os.path.join(log_dir, 'etl_process.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

#Función que extrae datos de wikipedia S&P 500 y crea el ticker
def extract_sp500(url):
    try:
        logging.info(f'Extrayendo datos de: {url}')    
        #Leer datos de la red
        table_sp500 = pd.read_html(url)[0]
        sp500 = table_sp500[['Símbolo', 'Seguridad', 'Sector GICS', 'Sub-industria GICS', 'Ubicación de la sede', 'Fundada']]
        sp500 = sp500.rename(columns={'Símbolo':'Symbol', "Seguridad": "Company_Name", 'Sector GICS': "Sector_GICS", 'Sub-industria GICS': "Sub_Sector_GICS", 'Ubicación de la sede': "Headquarters", 'Fundada': 'Fecha_Fundacion' })
        logging.info('Los datos han sido extraidos exitosamente')    
        

        #guardar el dataframe en un archivo csv
        filename = os.path.join(data_dir, f'Companies_profiles.csv')
        logging.info("Guardando el dataframe en un archivo csv")
        sp500.to_csv(filename, encoding="utf-8-sig", index=False, header=True)
        logging.info("Archivo csv guardado exitosamente")


        #Símbolos a lista para usarlo como ticker
        logging.info("Crear lista de Símbolos para usar como ticker")
        sp_symbols = sp500["Symbol"].tolist()
        logging.info("Lista creada correctamente")

        return  sp_symbols
 
    except Exception as e:
        logging.error(f'Error extrayendo datos de {url}: {e}')
        return None
    

#Función para extraer los datos
def extract_data(ticker, start_date, end_date):
    try:
        logging.info(f'Extrayendo datos para {ticker} desde {start_date} hasta {end_date}')
        data = yf.download(ticker, start=start_date, end=end_date)
        logging.info(f'Datos extraídos exitosamente para {ticker}')
        return data
    except Exception as e:
        logging.error(f'Error extrayendo datos para {ticker}: {e}')
        return None

#Función para transformar los datos
def transform_data(data):
    
    try:     
        logging.info('Transformando datos')
        transform_data = data['Adj Close'].reset_index().melt(id_vars=['Date'], value_vars=tickers, var_name='Symbol', value_name='Close')
        transform_data = transform_data.dropna()
        logging.info('Datos transformados exitosamente')        
        return transform_data

    except Exception as e:
        logging.error(f'Error transformando datos: {e}')
        return None


#Función para cargar los datos
def load_data(df):
    try:
        filename = os.path.join(data_dir, f'S&P500_prices_proccessed.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        df.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

#Esta función unifica todas las funciones anteriores
def etl_process(ticker, start_date, end_date):
    data = extract_data(ticker, start_date, end_date)
    if data is not None:
        transformed_data = transform_data(data)
        if transformed_data is not None:
            load_data(transformed_data)
            return transformed_data
    return None



#-------------------------------------------------------------#
#Etapa 3 Población de la Base de datos Modulo_3
#Primero debe crearse la BD el código para esto se encuentra en Creacion_BBDD
#Credenciales de la base de datos
server = 'SERVER'
database = 'DATABASE'
username = 'USER'
password = 'PASSWORD'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


#Inicio de la conexión
def ConexionBD():
    try:
        conn = pyodbc.connect(connection_string)
        logging.info(f'Conexión exitosa con la base de datos: {database}')
        return conn
    except Exception as e:  
        logging.error(f'Error generando conexion a la base de datos: {database}. {e}')
        return None
    

#Creación de la tabla
def CrearTablaBD(tabla, query):
    conn = None
    cursor = None
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        logging.info(f'Tabla: {tabla} creada de manera exitosa')
    except Exception as e:
        logging.error(f'Error al crear la tabla: {tabla}. {e}')
    if cursor:
        cursor.close()
    if conn:
        conn.close()


#Creacion de la llave primaria
def CrearPK(tabla, pk):
    conn = None
    cursor = None
    pk_columns = pk if isinstance(pk, str) else ', '.join(pk)
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        constraint = 'PK_' + tabla
        query = f'''ALTER TABLE {tabla} 
                ADD CONSTRAINT {constraint} PRIMARY KEY ({pk_columns})'''
        cursor.execute(query)
        cursor.commit()
        logging.info(f'Creada la llave primaria para la tabla: {tabla}')
    except Exception as e:
        logging.error(f'Error al crear la llave primaria de la tabla: {tabla}. {e}')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


#Creación de la función para poblar los datos en las tablas de la base de datos
def poblar_BBDD(datos, query):
    conn = None
    cursor = None
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        logging.info(f'Guardando datos de {datos} en la base de datos {database}')
        for _, row in datos.iterrows():
            cursor.execute(query, tuple(row.values))
        conn.commit()
        logging.info(f'Datos guardados correctamente en la base de datos: {database}')

    except Exception as e:
        logging.error(f'Error al cargar los datos de {datos} en la base de datos {database}: {e}')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == '__main__':

    url_sp = "https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500"
    tickers = extract_sp500(url_sp)
    start_date = '2024-01-01' 
    end_date = '2024-03-31'
    etl_process(tickers, start_date, end_date)


    #Crear tabla CompanyProfiles
    tabla = 'CompanyProfiles'
    query =  '''CREATE TABLE CompanyProfiles (
        Symbol VARCHAR(50) NOT NULL,
        Company VARCHAR(50),
        SectorGICS VARCHAR(50),
        SubSectorGICS VARCHAR(50),
        Headquarters VARCHAR(50),
        FechaFundacion VARCHAR(50)
            )'''
    pk = 'Symbol'
    CrearTablaBD(tabla, query)
    CrearPK(tabla, pk)

    #Crear tabla Companies
    tabla = 'Companies'
    query = '''CREATE TABLE Companies (
        DateCol DATE NOT NULL,
        Symbol VARCHAR(50) NOT NULL,
        ClosePrice FLOAT
    )'''
    pk = ('DateCol', 'Symbol')

    CrearTablaBD(tabla, query)
    CrearPK(tabla, pk)


    #Poblar datos en la tabla CompanyProfiles
    datos = pd.read_csv(r'data/Companies_profiles.csv')
    insert_query_1 = 'INSERT INTO CompanyProfiles (Symbol, Company, SectorGICS, SubSectorGICS, Headquarters, FechaFundacion) VALUES (?,?,?,?,?,?)'
    poblar_BBDD(datos, insert_query_1)


    #Poblar datos en la tabla Companies
    datos = pd.read_csv(r'data/S&P500_prices_proccessed.csv')
    insert_query_2 = 'INSERT INTO Companies (DateCol, Symbol, ClosePrice) VALUES (?,?,?)'
    poblar_BBDD(datos, insert_query_2)



#   /\_/\  (
#  ( ^.^ ) _)
#    \"/  (
#  ( | | )
# (__d b__)