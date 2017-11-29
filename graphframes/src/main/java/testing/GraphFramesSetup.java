package testing;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import static org.apache.spark.sql.functions.*;

public class GraphFramesSetup {
    private static List<Row> readCSV(String location, SparkSession session) throws Exception {
        return session.read().csv(location).collectAsList();
    }

    public static GraphFrame buildGraph(SparkSession session) throws Exception {
        // Load data first time
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());
        
        List<Row> telefonos = readCSV("/user/socamica/telefonos.csv", session);
        SubGraph telefonosGraph = GraphBuilder.buildTelefonos(telefonos);
        List<Row> llamadas = readCSV("/user/socamica/llamadas.csv", session);
        SubGraph llamadasGraph = GraphBuilder.buildLlamadas(llamadas);

        Dataset<Row> verticesDF = telefonosGraph.getVertices(context).union(llamadasGraph.getVertices(context));
        Dataset<Row> edgesDF = telefonosGraph.getEdges(context).union(llamadasGraph.getEdges(context));

        // Persist data
        edgesDF.write().save("/user/socamica/grupo1v1-edges");
        verticesDF.write().save("/user/socamica/grupo1v1-vertices");
        return GraphFrame.apply(verticesDF, edgesDF);
    }

    public static GraphFrame loadGraph(SparkSession session) {
        // Recover persisted data
        Dataset<Row> edgesDF = session.read().load("/user/socamica/grupo1v1-edges");
        Dataset<Row> verticesDF = session.read().load("/user/socamica/grupo1v1-vertices");
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
        
        GraphFrame myGraph = loadGraph(sp);
        benchmark(() -> {
            Dataset<Row> calls = myGraph.find("(t1)-[]->(l); (t2)-[]->(l); (u1)-[]->(t1); (u2)-[]->(t2)");
            Dataset<Row> result = calls.filter("l.type = 'llamada'")
                                       .filter(month(calls.col("l.startTime")).$eq$eq$eq(9))
                                       .filter("t1.type = 'telefono'")
                                       .filter("t2.type = 'telefono'")
                                       .filter("u1.id > u2.id")
                                       .groupBy(calls.col("u1.id"), calls.col("u2.id"))
                                       .agg(countDistinct(calls.col("l.id")).as("cantidad_llamadas"));
            result.show();
        });
        sp.close();
    }
}
