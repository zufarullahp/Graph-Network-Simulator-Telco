CALL gds.beta.pipeline.linkPrediction.create('pipe');

CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe', 'fastRP', {
  mutateProperty: 'embedding',
  embeddingDimension: 64,
  randomSeed: 42
});

CALL gds.beta.pipeline.linkPrediction.addFeature('pipe', 'hadamard', {
  nodeProperties: ['embedding']
}) YIELD featureSteps;

CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe', {
  testFraction: 0.25,
  trainFraction: 0.6,
  validationFolds: 3
})
YIELD splitConfig;

CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe')
YIELD parameterSpace;

CALL gds.beta.pipeline.linkPrediction.addRandomForest('pipe', {numberOfDecisionTrees: 10})
YIELD parameterSpace;

CALL gds.alpha.pipeline.linkPrediction.addMLP('pipe',
{hiddenLayerSizes: [4, 2], penalty: 0.5, patience: 2, classWeights: [0.55, 0.45], focusWeight: {range: [0.0, 0.1]}})
YIELD parameterSpace;

CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe', {maxEpochs: 500, penalty: {range: [1e-4, 1e2]}})
YIELD parameterSpace
RETURN parameterSpace.RandomForest AS randomForestSpace, parameterSpace.LogisticRegression AS logisticRegressionSpace, parameterSpace.MultilayerPerceptron AS MultilayerPerceptronSpace;

CALL gds.graph.project(
  'link_downlinks',
  '*',
  {
    LINKS: {
      orientation: 'UNDIRECTED',
      properties:'occupancy_downlink'
    }
  }
);

CALL gds.beta.pipeline.linkPrediction.train('link_downlinks', {
  pipeline: 'pipe',
  modelName: 'lp-pipeline-model3',
  metrics: ['AUCPR', 'OUT_OF_BAG_ERROR'],
  targetRelationshipType: 'LINKS',
  randomSeed: 12
}) YIELD modelInfo, modelSelectionStats
RETURN
  modelInfo.bestParameters AS winningModel,
  modelInfo.metrics.AUCPR.train.avg AS avgTrainScore,
  modelInfo.metrics.AUCPR.outerTrain AS outerTrainScore,
  modelInfo.metrics.AUCPR.test AS testScore,
  [cand IN modelSelectionStats.modelCandidates | cand.metrics.AUCPR.validation.avg] AS validationScores;

CALL gds.beta.pipeline.linkPrediction.predict.stream('link_downlinks', {
  modelName: 'lp-pipeline-model3',
  topN:100
})
 YIELD node1, node2, probability
WITH node1, node2, gds.util.asNode(node1).nameReal AS NE1, gds.util.asNode(node2).nameReal AS NE2, probability
where NE2 contains "ME"
 return node1, node2, NE1, NE2, probability
ORDER BY probability asc, NE1;

//// Check for reasoning

match path=(n)-[:LINKS*1..2]->() where id(n) = 0
match path2=(n2)-[:LINKS*1..2]->()  where id(n2) = 183
return path,path2
