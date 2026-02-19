import datetime
from pydataxm.pydatasimem import CatalogSIMEM
import pydataxm.pydataxm as api
import pandas as pd
from google.cloud import storage

catalogo_conjuntos = CatalogSIMEM('Datasets')
api=api.ReadDB()

#Aquí se aloja la información del conjunto de datos incluyendo el comienzo de registros hasta su final
df=catalogo_conjuntos.get_data()
conjunto_datos=df[df['nombreConjuntoDatos'].str.lower().str.contains('combus')]
#print(conjunto_datos)

#Aquí se encuentran las métricas o códigos para obtener los datos (No es el ID)
metrica=api.get_collections()
metric_description=metrica[metrica['MetricName'].str.lower().str.contains('demanda')]   #Filtrar nombres de tablas por palabra clave

fecha_inicio=datetime.date(2019,1,1)
fecha_fin=datetime.date(2025,12,31)

#Generación de emisiones de CO2
emisiones_co2=api.request_data('EmisionesCO2','RecursoComb',fecha_inicio,fecha_fin)
#Generación de KWh en las plantas
generacion_real=api.request_data('Gene','Recurso',fecha_inicio,fecha_fin)

consumo_combust=api.request_data('ConsCombAprox','RecursoComb',fecha_inicio,fecha_fin)
gene_fuera_merito=api.request_data('GeneFueraMerito','Recurso',fecha_inicio,fecha_fin)
precio_bolsa=api.request_data('PrecBolsNaci','Sistema',fecha_inicio,fecha_fin)
aportes=api.request_data('AporEner','Sistema',fecha_inicio,fecha_fin)
import_energia=api.request_data('ImpoEner','Sistema',fecha_inicio,fecha_fin)
demanda=api.request_data('DemaReal','Sistema',fecha_inicio,fecha_fin)

#Prepapar conexión a GCS
bucket_name='co2_emissions_enero'
storage_cliente=storage.Client()
bucket=storage_cliente.bucket(bucket_name)

blob=bucket.blob('establecer nombre de variable')
blob.upload_from_string(demanda.to_csv(index=False).encode("utf-8"),content_type='text/csv')




