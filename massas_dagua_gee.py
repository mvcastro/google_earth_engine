import ee
from datetime import datetime
import pandas as pd

ee.Initialize()

def retorna_lista(img, featCol):
  """Analisa cada imagem da coleção e retorna uma lista com os resultados
      sendo cada item da lista referente a uma data"""

  # Retorna uma FeatureCollection com o número de pixels dentro de cada polígono de massa d'água 
  data_count = img.reduceRegions(featCol, ee.Reducer.count(), 30)
  # Inserção da data da imagem analisada nas propriedades da FeatureCollection
  data_count = data_count.map(lambda feat: feat.set('data_analise', img.date().format()))
 
  feats_stats = data_count.getInfo()['features']

  return feats_stats


def retorna_dataframe(result):
    # Itens a serem retirados do resultado
    data_analise = []
    idx_pacote = []
    vx_vacum = []
    esp_cd = []
    nuareaha = []
    count = []
    mean = []
    median = []

    count_flag = False
    mean_flag = False
    median_flag = False

    for feat in result:
        data_analise.append(feat['properties']['data_analise'])
        idx_pacote.append(feat['properties']['IDX_PACOTE'])
        vx_vacum.append(feat['properties']['VXACUM'])
        esp_cd.append(feat['properties']['intESP_CD'])
        nuareaha.append(feat['properties']['nuareaha'])
        if 'count' in feat['properties']:
            count.append(feat['properties']['count'])
            count_flag = True
        if 'mean' in feat['properties']:
            mean.append(feat['properties']['mean'])
            mean_flag = True
        if 'median' in feat['properties']:
            median.append(feat['properties']['median'])
            median_flag = True

        # Criando um objeto dicionário com os resultados
    feature = {}
    feature['data_analise'] = data_analise
    feature['idx_pacote'] = idx_pacote
    feature['vx_vacum'] = vx_vacum
    feature['esp_cd'] = esp_cd
    feature['nuareaha'] = nuareaha
    if count_flag:
        feature['count'] = count
    if mean_flag:
        feature['mean'] = mean
    if median_flag:
        feature['median'] = median

    return pd.DataFrame(feature, index=data_analise)


def retorna_df_merged(lista_result):

    df_result_stats = retorna_dataframe(lista_result)
    return df_result_stats


if __name__ == "__main__":

    def mascara(img):
        mask = img.eq(2)
        maskedImage = img.updateMask(mask)
        return maskedImage 
    
    # Tempo inicial
    timeInMsBefore = datetime.now()

    # Importação do Shapefile com massas d'água
    massas_dagua = ee.FeatureCollection("users/mvcastro1975/pol_ScriptV3_08")
    # Lista de elementos da FeatureCollection
    lista_massas_dagua = massas_dagua.toList(massas_dagua.size())
		# Número de massa d'água no shapefile
    num_massas = massas_dagua.size().getInfo()

    # Processa 1000 polígonos por vez (pol_ScriptV3_08 possui 8.781 polígonos)
    for limite in range(1, int(num_massas/1000) + 2):

        # Lista com 1000 Features por vez (divide o shapefile a cada 1000 polígonos)
        lista_parcial = lista_massas_dagua.slice(
            (limite - 1) * 1000, limite * 1000)
        # Criando FeatureCollection com 1000 valores
        massas_dagua_parcial = ee.FeatureCollection(lista_parcial)

        # Anos a serem considerados no processamento
        anos = range(2002, 2003)
        # Meses do ano
        meses = range(1, 2)

        # Importa dataset com detcção de água para cada mês da série
        monthly_hist = ee.ImageCollection('JRC/GSW1_0/MonthlyHistory')\
                            .select('water').map(mascara)

        for ano in anos:
            for mes in meses:
                imagem = monthly_hist.filter(ee.Filter.And(\
                    ee.Filter.eq('year', ano),\
                    ee.Filter.eq('month', mes)))
                # Retorna resultado para a imagem na data em análise e as feições de massa d'água
                if imagem.size().getInfo() != 0:
                    lista_result = retorna_lista(imagem.first(), massas_dagua_parcial)
                    df_result = retorna_dataframe(lista_result)
                    # pasta escolhida para armazenamento dos dados
                    path = 'C:/Temp/GEE3/resultado_pol_ScriptV3_08_ano{}mes{}_parte{}.csv'.format(\
                            ano, mes, limite)
                    df_result.to_csv(path, sep=';', decimal=',')

    # Tempo final
    timeInMsAfter = datetime.now()
    print(timeInMsAfter - timeInMsBefore)
