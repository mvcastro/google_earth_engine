import ee
from datetime import datetime
import pandas as pd

ee.Initialize()

def retorna_lista(img, featCol):
  """Analisa cada imagem da coleção e retorna uma lista com os resultados
      sendo cada item da lista referente a uma data"""

  # Retorna uma FeatureCollection com o número de pixels dentro de cada polígono de massa d'água
  data_count = img.reduceRegions(featCol, ee.Reducer.count(), 500)
  # Inserção da data da imagem analisada nas propriedades da FeatureCollection
  data_count = data_count.map(lambda feat: feat.set('data_analise', img.date().format()))

  # Retona uma FeatureCollection com a média dos valores dos pixels inseridos
  #  em cada polígono de massa d'água
  data_mean = img.reduceRegions(featCol, ee.Reducer.mean(), 500)
  # Inserção da data da imagem analisada nas propriedades da FeatureCollection
  data_mean = data_mean.map(lambda feat: feat.set('data_analise', img.date().format()))

  # Retona uma FeatureCollection com a mediana dos valores dos pixels inseridos
  #  em cada polígono de massa d'água
  data_median = img.reduceRegions(featCol, ee.Reducer.median(), 500)
  # Inserção da data da imagem analisada nas propriedades da FeatureCollection
  data_median = data_median.map(lambda feat: feat.set('data_analise', img.date().format()))

  feats_count = data_count.getInfo()['features']
  feats_mean = data_mean.getInfo()['features']
  feats_median = data_median.getInfo()['features']

  return [feats_count, feats_mean, feats_median]


def retorna_dataframe(result):
    # Itens a serem retirados do resultado
    esp_cd = []
    data_analise = []
    data_const = []
    nooriginal = []
    count = []
    mean = []
    median = []

    # for feat in result['features']:
    for feat in result:
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

    return pd.DataFrame(feature, index=data_analise)


def retorna_df_merged(lista_result):

    count = lista_result[0]
    mean = lista_result[1]
    median = lista_result[2]

    df_result_count = retorna_dataframe(count)
    df_result_mean = retorna_dataframe(mean)
    df_result_median = retorna_dataframe(median)

    df_result = df_result_count\
        .merge(df_result_mean[['esp_cd', 'mean']], on='esp_cd')\
    .merge(df_result_median[['esp_cd', 'median']], on='esp_cd')

    return df_result


if __name__ == "__main__":

  #Tempo inicial
  timeInMsBefore = datetime.now()

  # Importação do Shapefile com massas d'água
  massas_dagua = ee.FeatureCollection("users/mvcastro1975/massa_dagua/massa_dagua_maior_100ha")

  anos = range(1984, 1985)
  meses = range(1, 5)

  # Importa dataset com recorrência mensal de água
  monthly_hist = ee.ImageCollection('JRC/GSW1_0/MonthlyHistory').select('water')

  resultado = []

  for ano in anos:
    for mes in meses:
      imagem = monthly_hist.filter(ee.Filter.And(\
                ee.Filter.eq('year', ano),\
                ee.Filter.eq('month', mes)))
      # Retorna resultado para a imagem na data em análise e as feições de massa d'água 
      if imagem.size().getInfo() != 0:
        lista_result = retorna_lista(imagem.first(), massas_dagua)
        df_result = retorna_df_merged(lista_result)
        #pasta onde serão armazenados os dados
        path = 'C:/Temp/GEE/resultado{}-{}.csv'.format(ano, mes)
        df_result.to_csv(path, sep=';')
  
  
  # Tempo final
  timeInMsAfter = datetime.now()
  print(timeInMsAfter - timeInMsBefore)
