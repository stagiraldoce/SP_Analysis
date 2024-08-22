# S&P 500 Analysis Project

## Descripción del proyecto

Este proyecto tiene como objetivo analizar las empresas del S&P 500 a través de diferentes fases, que incluyen la extracción de datos, análisis estadístico, almacenamiento en SQL Server, creación de dashboards en Power BI, y finalmente, una clusterización basada en la volatilidad de las acciones.

## Requisitos
- Python 3.x
- Librerías: `pandas`, `pyodbc`, `scikit-learn`, `matplotlib`, `yfinance`, `numpy`, `seaborn`
- Power BI Desktop
- SQL Server

## Estructura del Proyecto
- `data/`: Contiene los archivos CSV con los datos de las empresas y perfiles.
- `scripts/`: Contiene los scripts Python para las diferentes fases del proyecto.
- `dashboards/`: Contiene el archivo .pbix de Power BI.
- `README.md`: Documento explicativo del proyecto.

1. Clona este repositorio:
git clone https://github.com/stagiraldoce/SP_Analysis
cd SP_Analysis
2. Instala las dependencias necesarias: pip install -r requirements.txt
3. Configura la conexión a SQL Server en los scripts de las fases correspondientes.
4. Ejecuta los scripts en orden para realizar el análisis completo.

## Fases del proyecto

### Fase 1: Extracción de Datos

* Obtención de datos de empresas del S&P 500 desde Wikipedia.
* Descarga de los precios de cotización del último año.

### Fase 2: Análisis Estadístico

* Análisis descriptivo e inferencial de los precios de las acciones.

### Fase 3: Almacenamiento en SQL Server

* Carga de los datos limpios en una base de datos SQL Server.

### Fase 4: Dashboard en Power BI

* Creación de un dashboard interactivo con KPIs, tooltips y bookmarks.

### Fase 5: Clusterización de las Acciones

* Agrupamiento de las acciones en clusters según indicadores de volatilidad.

### Fase 6: Publicación en GitHub

* Subida del proyecto al repositorio de GitHub y documentación en este archivo README.md.

