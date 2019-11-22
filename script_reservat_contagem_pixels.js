//Função que transforma uma lista de valores grupos em um objeto ee.Dictionaty
function list2Dict(dict1, dict2){
  dict1 = ee.Dictionary(dict1);
  var grupo = ee.Number(ee.Dictionary(dict1).get('group'));
  dict2 = ee.Dictionary(dict2);
  
  dict2 = ee.Algorithms.If(grupo.eq(0), 
                dict2.set('group0Count', dict1.get('count')),
            ee.Algorithms.If(grupo.eq(1),
                dict2.set('group1Count', dict1.get('count')),
            ee.Algorithms.If(grupo.eq(2),
                dict2.set('group2Count', dict1.get('count')), 
              null)));
  return dict2;
}

//Função que gera uma lista de de ImageCollections para 
//os polígonos da FeatureCollection passada como parâmetro,
//gerando nas propriedades de cada polígono a informação do
//número de pixels iguais a 0 (NoData), 1(sem água) e 2(com água)
//presentes no interior da feição 
function featuresEstat(imgCollection, featCollec) {
  
  var listaResult = ee.List([]);
  
function estat(img, lista) {
  var newFeatCol = ee.FeatureCollection(featCollec).map(function(feat) {
    var dataCount = img.addBands(ee.Image.pixelArea()).reduceRegion(
                          {reducer: ee.Reducer.count().unweighted().group(),
                           geometry: feat.geometry(),
                           scale: 30}
                           );
    var dict = ee.Dictionary({});
    var asList = ee.List(dataCount.get('groups'));
    var newDict = ee.Dictionary(asList.iterate(list2Dict, dict));
    
    var grupo0 = ee.Algorithms.If(newDict.contains('group0Count'), newDict.get('group0Count'), 0);
    var grupo1 = ee.Algorithms.If(newDict.contains('group1Count'), newDict.get('group1Count'), 0);
    var grupo2 = ee.Algorithms.If(newDict.contains('group2Count'), newDict.get('group2Count'), 0);
    
    feat =  feat.set('group0Count', grupo0)
                .set('group1Count', grupo1)
                .set('group2Count', grupo2)
                .set('data', img.date().format())
                .set('esp_cd', feat.get('intESP_CD'));
    return feat;
  });
  
  return ee.List(lista).add(newFeatCol);
}
  var resultado = imgCollection.iterate(estat, listaResult);
  
  return resultado;
}

/*-----------------------------------------------------------------
** Testando as funções **
-------------------------------------------------------------------
*/

//Importação do Shapefile com massas d'água
var featCol = ee.FeatureCollection("users/mvcastro1975/pol_ScriptV3_08");

//Importa dataset com recorrência mensal de água
var monthly_hist = ee.ImageCollection('JRC/GSW1_0/MonthlyHistory').select('water');

//Selecionando os 100 primeiros polígonos do shapefile para teste
var massas_sel = featCol.limit(100, 'nuareaha', false);

//Gerando uma lista de FeatureCollections
var resultado = ee.List(featuresEstat(monthly_hist, massas_sel));

//Transforma a lista de FeatureCollections em uma lista de Features
var listOfListOfFeatures = ee.List([]);
function featCol2List(feat, lista){
  return ee.List(lista).add(ee.Feature(feat));
}
var listOfLists = resultado.map(function(fc){
    var fr = ee.FeatureCollection(fc).iterate(featCol2List, listOfListOfFeatures);
    return ee.List(fr);
});

//Gera uma FeatureCollection com a lista de Features
var finalFeatCol = ee.FeatureCollection(listOfLists.flatten());

//Exporta a FeatureCollection resultante
//Para cada polígono tem-se a contagem de de pi
Export.table.toAsset(finalFeatCol);
