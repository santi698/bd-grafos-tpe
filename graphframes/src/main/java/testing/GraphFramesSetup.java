package testing;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import static org.apache.spark.sql.functions.*;

public class GraphFramesSetup {
    private static String CALLS_FILE_LOCATION = "/user/socamica/llamadas800K.csv";
    private static String PHONES_FILE_LOCATION = "/user/socamica/telefonos800K.csv";
    private static String VERTICES_PARQUET_LOCATION = "/user/socamica/grupo1v2-vertices";
    private static String EDGES_PARQUET_LOCATION = "/user/socamica/grupo1v2-edges";
    private static boolean BUILD_GRAPH = false;
    private static String QUERY_NUMBER = "1_1";
    private static List<Row> readCSV(String location, SparkSession session, int offset, int limit) throws Exception {
        Dataset<Row> csv = session.read().csv(location);
        return csv.filter(new FilterFunction<Row>(){
            @Override
            public boolean call(Row row) throws Exception {
                return Long.valueOf((String) row.get(0)) >= offset && Long.valueOf((String) row.get(0)) < (offset + limit);
            }
        }).collectAsList();
    }

    public static GraphFrame buildGraph(SparkSession session) throws Exception {
        // Load data first time
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());
        int offset = 0;
        int limit = 100000;
        List<Row> chunkPhones;
        List<Row> chunkCalls;
        chunkPhones = readCSV(PHONES_FILE_LOCATION, session, offset, limit);
        SubGraph telefonosGraph = GraphBuilder.buildTelefonos(chunkPhones);
        Dataset<Row> verticesDF = telefonosGraph.getVertices(context);
        Dataset<Row> edgesDF = telefonosGraph.getEdges(context);
        chunkCalls = readCSV(CALLS_FILE_LOCATION, session, offset, limit);
        while(chunkCalls.size() > 0) {
            SubGraph llamadasGraph = GraphBuilder.buildLlamadas(chunkCalls);
            verticesDF = verticesDF.union(llamadasGraph.getVertices(context));
            edgesDF = edgesDF.union(llamadasGraph.getEdges(context));
            offset += limit;
            chunkCalls = readCSV(CALLS_FILE_LOCATION, session, offset, limit);
        }
        // Persist data
        edgesDF.write().save(EDGES_PARQUET_LOCATION);
        verticesDF.write().save(VERTICES_PARQUET_LOCATION);
        return GraphFrame.apply(verticesDF, edgesDF);
    }

    public static GraphFrame loadGraph(SparkSession session) {
        // Recover persisted data
        Dataset<Row> edgesDF = session.read().load(EDGES_PARQUET_LOCATION);
        Dataset<Row> verticesDF = session.read().load(VERTICES_PARQUET_LOCATION);
        return GraphFrame.apply(verticesDF, edgesDF);
    }

    public static void benchmark(Runnable func) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            long t1 = System.currentTimeMillis();
            func.run();
            System.out.println("Took: " + (System.currentTimeMillis() - t1));
        }
        Long time = (System.currentTimeMillis() - start);
        System.out.println("[TOTAL] Took: " + time);
        System.out.println("[TOTAL] Average: " + time / 5.0);
    }

    public static Dataset<Row> runQuery(GraphFrame graph, String number) {
        Dataset<Row> calls;
        switch(number) {
            case "1_1":
                calls = graph.find("(t1)-[]->(l); (t2)-[]->(l)");
                return calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("t1.numero > t2.numero")
                            .groupBy("t1.numero", "t2.numero")
                            .agg(avg(calls.col("l.duration")));
            case "1_2":
                calls = graph.find("(t1)-[e1]->(l); (t2)-[e2]->(l)");
                return calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("t1.id > t2.id")
                            .filter("e1.type = 'creo'")
                            .filter("e2.type = 'recibio'")
                            .groupBy("t1.id", "t2.id")
                            .agg(avg(calls.col("l.duration")));
            case "1_3":
                calls = graph.find("(t1)-[]->(l); (t2)-[]->(l)");
                return calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("t1.numero > t2.numero")
                            .groupBy("t1.numero", "t2.numero")
                            .agg(max(calls.col("l.duration")));
            case "1_4":
                calls = graph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
                return calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .groupBy("u1.id", "u2.id")
                            .agg(countDistinct(calls.col("l.id").as("cantidad_llamadas")));
            case "1_5":
                calls = graph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
                return calls.filter("l.type = 'llamada'")
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .groupBy(calls.col("u1.id"), calls.col("u2.id"),
                                     month(calls.col("l.startTime")).as("mes"),
                                     year(calls.col("l.startTime")).as("año"))
                            .agg(countDistinct(calls.col("l.id")).as("cantidad_llamadas"));
            case "1_6":
                calls = graph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
                return calls.filter("l.type = 'llamada'")
                            .filter(month(calls.col("l.startTime")).$eq$eq$eq(9))
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .groupBy(calls.col("u1.id"), calls.col("u2.id"))
                            .agg(countDistinct(calls.col("l.id")).as("cantidad_llamadas"));
            case "2_1":
                calls = graph.find("(t1)-[]->(l); (t2)-[]->(l); (t3)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2); (u3)-[]->(t3)");
                return calls.filter("l.type = 'llamada'")
                            .filter(month(calls.col("l.startTime")).$eq$eq$eq(9))
                            .filter(year(calls.col("l.startTime")).$eq$eq$eq(2017))
                            .filter("t1.type = 'telefono'")
                            .filter("t2.type = 'telefono'")
                            .filter("t3.type = 'telefono'")
                            .filter("u1.id > u2.id")
                            .filter("u2.id > u3.id")
                            .groupBy("u1.id", "u2.id", "u3.id")
                            .agg(avg(calls.col("l.duration")));
            default: return null;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkSession sp = SparkSession
            .builder()
            .appName("TPE Grupo 1")
            .getOrCreate();
        
            GraphFrame myGraph;
        if (BUILD_GRAPH) {
            myGraph = buildGraph(sp);
        } else {
            myGraph = loadGraph(sp);
        }
        long start = System.currentTimeMillis();
        Dataset<Row> result = runQuery(myGraph, QUERY_NUMBER);
        long elapsed = System.currentTimeMillis() - start;
        result.show();
        System.out.println("\n\n\n\n\nThe query took " + elapsed + " ms.\n\n\n\n\n");
        sp.close();
    }
}
