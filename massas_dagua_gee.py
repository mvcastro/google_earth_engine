import ee
from datetime import datetime
import pandas as pd

ee.Initialize()

#Importação do Shapefile com massas d'água
table = ee.FeatureCollection("users/mvcastro1975/massa_dagua/massa_dagua_maior_100ha")

#Importa dataset com recorrência mensal de água
monthly_hist = ee.ImageCollection('JRC/GSW1_0/MonthlyHistory').select('water')


def retorna_lista(img, featCol):
  
    data_count = img.reduceRegions(featCol, ee.Reducer.count(), 500)
    data_count = data_count.map(lambda feat: feat.set('data_analise', image_fst.date().format()))
     
    data_mean = img.reduceRegions(featCol, ee.Reducer.mean(), 500)
    data_mean = data_mean.map(lambda feat: feat.set('data_analise', image_fst.date().format()))

    data_median = img.reduceRegions(featCol, ee.Reducer.median(), 500)
    data_median = data_median.map(lambda feat: feat.set('data_analise', image_fst.date().format()))
        
    return [data_count.getInfo(), data_mean.getInfo(), data_median.getInfo()]
    
def retorna_dataframe(result):
  #Itens a serem retirados do resultado
  esp_cd = []
  data_analise = []
  data_const = []
  nooriginal = []
  count = []
  mean = []
  median = []

  for feat in result['features']:
    esp_cd.append(feat['properties']['esp_cd'])
    data_analise.append(feat['properties']['data_analise'])
    data_const.append(feat['properties']['data_const'])
    nooriginal.append(feat['properties']['nooriginal'])
    if 'count' in feat['properties']:
      count.append(feat['properties']['count'])
    if 'mean' in feat['properties']:
      mean.append(feat['properties']['mean'])
    if 'median' in feat['properties']:
      median.append(feat['properties']['median'])

  feature = {}
  feature['esp_cd'] = esp_cd
  feature['data_analise'] = data_analise
  feature['data_const'] = data_const
  feature['nooriginal'] = nooriginal
  if 'count' in feat['properties']:
    feature['count'] = count
  if 'mean' in feat['properties']:
    feature['mean'] = mean
  if 'median' in feat['properties']:
    feature['median'] = median

  return  pd.DataFrame(feature, index=[feature['data_analise']])

def retorna_df_merged(lista_result):
  
  df_result_count = retorna_dataframe(lista_result[0])
  df_result_mean = retorna_dataframe(lista_result[1])
  df_result_median = retorna_dataframe(lista_result[2])
    
  df_result = df_result_count\
                .merge(df_result_mean[['esp_cd', 'mean']], on='esp_cd')\
                .merge(df_result_median[['esp_cd','median']], on='esp_cd')

  return df_result

if __name__ == "__main__":
    
  image_fst = monthly_hist.first()
  timeInMsBefore = datetime.now()
  colecao = table #.filterMetadata('nuvolumhm3', "less_than", 1)
  lista_result = retorna_lista(image_fst, colecao)
  df_result = retorna_df_merged(lista_result)
  df_result.to_excel("C:/Temp/output.xlsx")
  print(df_result)
  timeInMsAfter = datetime.now()
  print(timeInMsAfter - timeInMsBefore)
