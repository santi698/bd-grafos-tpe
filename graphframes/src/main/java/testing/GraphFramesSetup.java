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
        chunkPhones = readCSV("/user/socamica/telefonos800K.csv", session, offset, limit);
        SubGraph telefonosGraph = GraphBuilder.buildTelefonos(chunkPhones);
        Dataset<Row> verticesDF = telefonosGraph.getVertices(context);
        Dataset<Row> edgesDF = telefonosGraph.getEdges(context);
        chunkCalls = readCSV("/user/socamica/llamadas800K.csv", session, offset, limit);
        while(chunkPhones.size() > 0) {
            SubGraph llamadasGraph = GraphBuilder.buildLlamadas(chunkCalls);
            verticesDF = verticesDF.union(llamadasGraph.getVertices(context));
            edgesDF = edgesDF.union(llamadasGraph.getEdges(context));
            offset += limit;
            chunkCalls = readCSV("/user/socamica/llamadas800K.csv", session, offset, limit);
        }
        edgesDF.write().save("/user/socamica/grupo1v2-edges");
        verticesDF.write().save("/user/socamica/grupo1v2-vertices");
        // Persist data
        return GraphFrame.apply(verticesDF, edgesDF);
    }

    public static GraphFrame loadGraph(SparkSession session) {
        // Recover persisted data
        Dataset<Row> edgesDF = session.read().load("/user/socamica/grupo1v2-edges");
        Dataset<Row> verticesDF = session.read().load("/user/socamica/grupo1v2-vertices");
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

    public static void main(String[] args) throws Exception {
        SparkSession sp = SparkSession
            .builder()
            .appName("TPE Grupo 1")
            .getOrCreate();
        
        GraphFrame myGraph = buildGraph(sp);
        //benchmark(() -> {
        //    Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
        //    Dataset<Row> result = calls.filter("l.type = 'llamada'")
        //                               .filter(month(calls.col("l.startTime")).$eq$eq$eq(9))
        //                               .filter("t1.type = 'telefono'")
        //                               .filter("t2.type = 'telefono'")
        //                               .filter("u1.id > u2.id")
        //                               .groupBy(calls.col("u1.id"), calls.col("u2.id"))
        //                               .agg(countDistinct(calls.col("l.id")).as("cantidad_llamadas"));
        //    result.show();
        //});
        sp.close();
    }
}
