## SETUP

```sql
CREATE TABLE grupo1v1.telefono (
  id INTEGER,
  id_usuario INTEGER,
  numero VARCHAR,
  ciudad VARCHAR,
  pais VARCHAR,
  proveedor VARCHAR,
  PRIMARY KEY(id)
);

CREATE TABLE grupo1v1.llamada (
  id_llamada INTEGER,
  hora_inicio TIMESTAMP,
  duracion FLOAT,
  id_creador INTEGER REFERENCES grupo1v1.telefono (id),
  id_participante INTEGER REFERENCES grupo1v1.telefono (id)
);


CREATE INDEX pares_de_telefonos ON grupo1v1.llamada (id_creador, id_participante);
```

## Queries

Q1_1.

```sql
SELECT CASE WHEN id_creador < id_participante
            THEN (creador.numero, participante.numero)
            ELSE (participante.numero, creador.numero)
            END AS par,
       AVG(duracion)
FROM grupo1v3.llamada,
     grupo1v3.telefono creador,
     grupo1v3.telefono participante
WHERE creador.id = grupo1v3.llamada.id_creador
  AND participante.id = grupo1v3.llamada.id_participante
GROUP BY par;
```

Q1_2.

```sql
SELECT creador.numero, participante.numero,
       AVG(duracion)
FROM llamada,
     telefono creador,
     telefono participante
WHERE creador.id = llamada.id_creador
  AND participante.id = llamada.id_participante
GROUP BY creador.id, participante.id;
```

Q1_3.

```sql
SELECT CASE WHEN creador.id_usuario > participante.id_usuario
            THEN (creador.id_usuario, participante.id_usuario)
            ELSE (participante.id_usuario, creador.id_usuario)
            END AS par,
       MAX(duracion)
FROM llamada,
     telefono creador,
     telefono participante
WHERE creador.id = llamada.id_creador
  AND participante.id = llamada.id_participante
GROUP BY par;
```

Q1_4.

```sql
SELECT CASE WHEN creador.id_usuario > participante.id_usuario
            THEN (creador.id_usuario, participante.id_usuario)
            ELSE (participante.id_usuario, creador.id_usuario)
            END AS par,
       COUNT(*)
FROM llamada,
     telefono creador,
     telefono participante
WHERE creador.id = llamada.id_creador
  AND participante.id = llamada.id_participante
GROUP BY par;
```

Q1_5.

```sql
SELECT CASE WHEN creador.id_usuario > participante.id_usuario
            THEN (creador.id_usuario, participante.id_usuario)
            ELSE (participante.id_usuario, creador.id_usuario)
            END AS par,
       COUNT(*),
       EXTRACT (month FROM hora_inicio) AS mes
FROM llamada,
     telefono creador,
     telefono participante
WHERE creador.id = llamada.id_creador
  AND participante.id = llamada.id_participante
GROUP BY par, mes;
```

Q1_6.

```sql
SELECT CASE WHEN creador.id_usuario > participante.id_usuario
            THEN (creador.id_usuario, participante.id_usuario)
            ELSE (participante.id_usuario, creador.id_usuario)
            END AS par,
       COUNT(*)
FROM llamada,
     telefono creador,
     telefono participante
WHERE creador.id = llamada.id_creador
  AND participante.id = llamada.id_participante
  AND EXTRACT (month FROM hora_inicio) = 4
  AND EXTRACT(year FROM hora_inicio) = 2017
GROUP BY par;
```

Q2_1.

```sql
SELECT t1.id_usuario, t2.id_usuario, t3.id_usuario, AVG(l1.duracion)
FROM grupo1v1.llamada l1, grupo1v1.llamada l2, grupo1v1.llamada l3,
     grupo1v1.telefono t1, grupo1v1.telefono t2, grupo1v1.telefono t3
WHERE l1.id_llamada = l2.id_llamada AND l2.id_llamada = l3.id_llamada
  AND (
        l1.id_creador = t1.id
    AND l1.id_participante = t2.id
    AND l2.id_participante = t3.id
    AND t1.id_usuario <> t2.id_usuario
    AND t2.id_usuario <> t3.id_usuario
    AND t1.id_usuario <> t3.id_usuario
  ) OR (
        l1.id_participante = t1.id
    AND l2.id_participante = t2.id
    AND l3.id_participante = t3.id
    AND t1.id_usuario <> t2.id_usuario
    AND t2.id_usuario <> t3.id_usuario
    AND t1.id_usuario <> t3.id_usuario
  )
GROUP BY t1.id_usuario, t2.id_usuario, t3.id_usuario, l1.id_llamada;
```

# GraphFrames

Q1_1.

```java
Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                           .filter("t1.type = 'telefono'")
                           .filter("t2.type = 'telefono'")
                           .filter("t1.numero > t2.numero")
                           .groupBy("t1.numero", "t2.numero")
                           .agg(avg(calls.col("l.duration"));
```

Q1_2.

```java
Dataset<Row> calls = myGraph.find("(t1)-[e1]->(l); (t2)-[e2]->(l)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                           .filter("t1.type = 'telefono'")
                           .filter("t2.type = 'telefono'")
                           .filter("t1.id > t2.id")
                           .filter("e1.type = 'creo'")
                           .filter("e2.type = 'recibio'")
                           .groupBy("t1.id", "t2.id")
                           .agg(avg(calls.col("l.duration")));
```

Q1_3.

```java
Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                           .filter("t1.type = 'telefono'")
                           .filter("t2.type = 'telefono'")
                           .filter("t1.numero > t2.numero")
                           .groupBy("t1.numero", "t2.numero")
                           .agg(max(calls.col("l.duration"));
```

Q1_4.

```java
Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .groupBy("u1.id", "u2.id")
                            .agg(countDistinct(calls.col("l.id").as("cantidad_llamadas")));
```

Q1_5.

```java
Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .groupBy(calls.col("u1.id"), calls.col("u2.id"), month(calls.col("l.startTime")).as("mes"), year(calls.col("l.startTime")).as("año"))
                            .agg(countDistinct(calls.col("l.id")).as("cantidad_llamadas"));
```

Q1_6.

```java
Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                            .filter(month(calls.col("l.startTime")).$eq$eq$eq(9))
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .groupBy(calls.col("u1.id"), calls.col("u2.id"))
                            .agg(countDistinct(calls.col("l.id")).as("cantidad_llamadas"));
```

Q2_1.

```java
Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l); (t3)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2); (u3)-[]->(t3)");
Dataset<Row> result = calls.filter("l.type = 'llamada'")
                            .filter(month(calls.col("l.startTime")).$eq$eq$eq(9))
                            .filter(year(calls.col("l.startTime")).$eq$eq$eq(2017))
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("t3.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .filter("u2.id > u3.id")
                            .groupBy("u1.id", "u2.id", "u3.id")
                            .agg(avg(unix_timestamp(calls.col("l.endTime")).$minus(unix_timestamp(calls.col("l.startTime")));
```
