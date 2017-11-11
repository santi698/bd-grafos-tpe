## SETUP

```sql
CREATE TABLE grupo1v3.telefono (
  id integer,
  id_usuario integer,
  numero varchar,
  ciudad varchar,
  pais varchar,
  proveedor varchar,
  PRIMARY KEY(id)
);

CREATE TABLE grupo1v3.llamada (
  id_llamada integer,
  hora_inicio timestamp,
  hora_fin timestamp,
  id_creador integer REFERENCES grupo1v3.telefono (id),
  id_participante integer REFERENCES grupo1v3.telefono (id)
);


CREATE INDEX pares_de_telefonos
ON grupo1v3.llamada (id_creador, id_participante);

INSERT INTO telefono
VALUES
(
  1,
  1,
  '12341243',
  'Buenos Aires',
  'Argentina',
  'Movistar'
),
(
  2,
  2,
  '42314155',
  'Córdoba',
  'Argentina',
  'Claro'
),
(
  3,
  1,
  '43212341',
  'Buenos Aires',
  'Argentina',
  'Tuenti'
);

INSERT INTO llamada
VALUES
(
  1,
  '2017-10-02 10:12:21',
  '2017-10-02 10:21:10',
  1,
  2
),
(
  2,
  '2017-10-02 12:01:57',
  '2017-10-02 12:32:54',
  2,
  1
),
(
  3,
  '2017-10-03 09:58:25',
  '2017-10-03 10:09:47',
  1,
  2
),
(
  4,
  '2017-04-27 11:43:01',
  '2017-04-27 11:44:43',
  3,
  2
);
```
## Queries
Q1_1.
```sql
SELECT CASE WHEN id_creador < id_participante
            THEN (creador.numero, participante.numero)
            ELSE (participante.numero, creador.numero)
            END AS par,
       AVG(hora_fin - hora_inicio)
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
       AVG(hora_fin - hora_inicio)
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
       MAX(hora_fin - hora_inicio)
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
SELECT t1.id_usuario, t2.id_usuario, t3.id_usuario, AVG(l1.hora_fin - l1.hora_inicio)
FROM grupo1v3.llamada l1, grupo1v3.llamada l2, grupo1v3.llamada l3,
     grupo1v3.telefono t1, grupo1v3.telefono t2, grupo1v3.telefono t3
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
