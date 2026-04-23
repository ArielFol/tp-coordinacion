# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Informe

Para la elaboración de este trabajo se fueron resolviendo progresivamente distintos desafíos en la coordinación del procesamiento distribuido.

### Decisiones de diseño e Iteraciones

#### Múltiples clientes:

Para la coordinación de múltiples clientes, se agregó a cada message_handler un uuid para la identificación de la query, el cual se propagó por los mensajes. Usando este identificador, tanto el Sum como el AggregationFilter agrupan los resultados por query_id.

#### Múltiples clientes + Sum replicado:

En este caso, a lo implementado anteriormente se sumaron varios cambios:

- Propagación del EOF: al tener múltiples Sum que consumen de la misma cola, solo uno de estos lee el EOF. Para notificar esto a los demás Sum, el lector lo broadcastea por medio de un RabbitMQ Exchange. Este se consume en un trhead a parte, ya que, al ser una operación bloqueante, la concurrencia/paralelismo no se ve perjudicada por el Global Interpreter Lock de Python
- Query distribuida: Al tener varios Sum, una misma query se ve repartida entre los mismos. Esto presenta un problema: determinar cuándo el AggregationFilter cuenta con la información necesaria para enviar el mensaje al join. Para esto, mantiene la cantidad de EOF recibidos de Sum diferentes para una misma query y recién cuando son igual al MAX_SUM, envia los resultados al Join.
- EOF de Broadcast recibido mientras se procesan datos de la misma query: Para solucionar esta situación, se agregó un lock para evitar que no se llegue a procesar antes de enviar el EOF. También se agregó una variable para evitar EOF duplicados al aggregationFilter. Esta variable se descarta una vez procesada toda la query.

Esta implementación inicial se realizó con prefetch = 1.

#### Múltiples clientes + Múltiples réplicas:

A lo implementado anteriormente, se sumó una division de palabras según un hash determinístico (cada palabra siempre va a un único AggregationFilter). Además, similar a lo realizado para la sincronización del EOF, el Join espera a recibir tantos tops parciales como AggregationFilter hay, para obtener el top total y devolvérselo al usuario.

#### Implementación sin prefetch=1:

Para esto se tuvieron que realizar varios cambios. Primero que nada, se modificó el middleware para que sea thread-safe (con el prefetch de 1 no se daba el error con la frecuencia suficiente como para observarlo en las pruebas). Para esto se separaron tanto el channel como la connection para el consumer y el publisher, evitando así que 2 threads distintos accedieran al mismo, rompiendo el exchange/queue.

Luego, se cambió completamente el protocolo de sincronización, con el fin de contemplar mensajes procesados por el Sum, enteramente luego del envío del EOF al AggregationFilter. Como los AggregationFilter reciben solo una parte de las palabras, se agregó un campo al EOF con la cantidad total de registros por palabra (para una query en particular), y a cada palabra enviada por un Sum se le sumó la cantidad de registros que conformaron la suma. Con esto, el AggregationFilter puede determinar si una query fue procesada al completo si la cantidad de registros recibidos para cada palabra coincide con el total de la query.

Por otro lado, el Sum, a partir de recibido el EOF para una query, no libera variables y sigue enviando inmediatamente (sin acumular) todos los mensajes procesados pertenecientes a la misma. Esto con el fin de evitar posibles pérdidas de mensajes rezagados luego de la llegada del EOF. Finalmente, el Join, cuando recibe todos los tops parciales de una query, hace un broadcast (por medio del mismo exchange de control utilizado para la propagación de EOF) a todos los Sum para notificar que no llegarán más mensajes para dicha query y que pueden liberar recursos asociados a la misma.

### Arquitectura Final

  #### Coordinación Sum Aggregation:
  
  La coordinación se realiza mediante mensajes identificados con un query_id único, permitiendo que múltiples queries puedan procesarse sin interferir. 

  Al finalizar una query con un EOF, el Sum encargado de su lectura lo propaga entre todas las réplicas por medio de un exchange de control, con el fin de que todas las instancias se enteren del final de la consulta.

  Cada AggregationFilter recibe palabras según una función hash determinística, haciendo que una palabra siempre sea procesada por la misma réplica. Para determinar que se tienen todos los datos provenientes de los Sum, se verifican tanto los EOF recibidos como la cantidad de registros recibidos por palabra. Una vez procesada completamente la query, envían su resultado al Join para que devuelva el top general al cliente y notifique a los Sum del cierre de la query (por medio del exchange de control). 

  #### Escalabilidad respecto a Clientes:

  El sistema escala respecto a clientes mediante query_id, permitiendo mantener un estado separado por cliente. De esta forma se pueden procesar de forma concurrente múltiples requests.

  #### Escalabilidad respecto a controladores (Sum/AggregationFilter):

    Escala horizontalmente a medida que se agregan nuevas instancias de Sum, distribuyendo la carga de procesamiento e incrementando la capacidad de trabajo.

    Igualmente, se pueden agregar instancias de AggregationFilter. A diferencia del caso anterior, donde la distribución era "aleatoria", en este caso se realiza por medio de un hash determinístico que matchea cada palabra con una réplica particular, balanceando la carga y permitiendo un mayor procesamiento paralelo.

    En ambos casos, al aumentar la cantidad de réplicas, se mejora el throughput general del sistema.