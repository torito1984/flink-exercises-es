## Flink DOJO

Conjunto de ejerecicios de Flink para un caso simulado de IOT. Durante los ejercicios, se explora el API de Flink version 1.9 (Aug 2019) al completo,
tanto para el API de bajo nivel, Table y SQL. Se deja abierto una conexion a wikipedia para explorar un caso de uso libre.

El proyecto esta autocontenido y es posible ejecutar los trabajos tanto de manera local en una JVM o en un JobManager local
de Flink 1.9. Para ejecutar un JobManager local consulte la pagina del manual
https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/cluster_setup.html

## Ejemplo de ejecucion

Antes de ejecutar cualquier ejemplo, debe compilar el codigo con maven.

mvn clean install

La linea de comandos para ejecutar cualquiera de los ejemplos de streaming se puede ver en execute_sample.sh

## Ejercicio de Queryable State

El ejercicio de Queryable State require conocer el jobId del trabajo que vamos a consultar el estado. Para que esto funcione,
es neceario que el proxy de consulta de estado este funcionando. Las instrucciones para un cluster mononodo se pueden
encontrar en  https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/queryable_state.html en la seccion
"Activating Queryable State". Fundamentalmente, consiste en 2 pasos:

1. Editar el flink-config.yaml con la propiedad queryable-state.enable a true
2. Mover el jar flink-queryable-state-runtime_2.11-1.9.0.jar de la carpeta opt/ a la carpeta lib/
3. ./bin/start-cluster.sh

En caso de no poder conectar, buscar en localhost:8081 la linea que ponga "Started the Queryable State Proxy Server @ ..."
para conocer la IP y puertos en los que el Proxy de Estado esta escuchando.


## Orden de lectura de los ejemplos

Se sugiere seguir el siguiente orden al leer los ejemplos, aunque todos son autocontenidos:

1. FlinkHelloWorld
2. BasicPrintJob
3. ToFahrenheit
4. FahrenheitNCelsius
5. HeatAlert
6. LatestObservation
7. MaxRegisteredTemperature
8. MaxRegisteredTemperatureAggregation
9. ATemperatureDeprecated
10. ATemperature
11. MATemperatureProcessingTime
12. MATemperatureEventTime
13. SecondarySensor
14. EfficientSecondarySensor
15. MeanPerformanceTracking
16. StateTemperatureTracking
17. VehicleStateTracking
18. async.VehicleOwnerAsyncEnrichmentJob
19. CustomerServiceJob
20. cep.DangerousValves
21. table.StolenCarsJob
22. sql.SQLCustomerServiceJob
23. queryable.QueryableCustomerServiceJob
24. queryable.CustomerServiceClient
25. wikiedits.WikipediaAnalysis
