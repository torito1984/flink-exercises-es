package helloWorld;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Ejercicio 1: Bienvenidos a Flink!
 *
 * En este ejercicio se muestran los elementos mas basicos de un programa en Flink.
 */
public class FlinkHelloWorld {

    public static void main(String[] args) throws Exception {

        /**
         * En primer lugar tenemos que abrir un Job Manager que se encargara de gestionar el trabajo con uno o mas
         * TaskManager. Dependiendo de si es un trabajo Batch o Stream, abrimos un entorno de ejecucion u otro.
         * Es de esperar que en la version 1.10 esto sea transparente y solo exista un API
         */
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * Lo siguiente es introducir la fuentes de datos del programa. En un programa de Streaming estos son
         * DataStreams. Estos pueden venir de monitorizar un sistema de ficheros, Kafka, una Base de Datos relacional,...
         * Existen conectores para la mayor parte de sistemas habituales.
         *
         * En este caso generaremos un DataStream a partir de una lista en memoria (ideal para test).
         */
        DataStream<String> greetings = see.fromCollection(Lists.newArrayList("Hello", "Apache", "Flink"));

        /**
         * La siguiente pieza fundamental en un programa de streaming son las acciones a tomar. Por lo general, los resultados
         * se publicaran en otros sistemas de salida. En la vesion 1.9 tenemos la opcion de servir el estado interno de
         * una aplicacion de Flink directamente (ver ejemplo QueryableCustomerServiceJob mas adelante.
         */
        greetings.print();

        /**
         * Una vez que todas las fuentes de datos, operadores y acciones estan definidos, se llama a execute. Desde este
         * momento la aplicacion se mantendra ejecutando continuamente hasta que se apague. La ejecucion ocurre en 2 pasos
         *  1. El plan de ejecucion programado es optimizado en un DAG de ejecucion optimo
         *  2. Las distintas tareas se reparten entre los TaskManager disponibles
         *  3. Se comienza a consumir datos de las fuentes y ejecuta el pipeline.
         */
        see.execute();
    }
}
