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
        
        List<Row> telefonos = readCSV("/user/socamica/telefonos_100K.csv", session);
        SubGraph telefonosGraph = GraphBuilder.buildTelefonos(telefonos);
        List<Row> llamadas = readCSV("/user/socamica/llamadas_100K.csv", session);
        SubGraph llamadasGraph = GraphBuilder.buildLlamadas(llamadas);

        Dataset<Row> verticesDF = telefonosGraph.getVertices(context).union(llamadasGraph.getVertices(context));
        Dataset<Row> edgesDF = telefonosGraph.getEdges(context).union(llamadasGraph.getEdges(context));

        // Persist data
        edgesDF.write().save("/user/socamica/grupo1v2-edges");
        verticesDF.write().save("/user/socamica/grupo1v2-vertices");
        return GraphFrame.apply(verticesDF, edgesDF);
    }

    public static GraphFrame loadGraph(SparkSession session) {
        // Recover persisted data
        Dataset<Row> edgesDF = session.read().load("/user/socamica/grupo1v2-edges");
        Dataset<Row> verticesDF = session.read().load("/user/socamica/grupo1v2-vertices");
        return GraphFrame.apply(verticesDF, edgesDF);
    }

    public static void main(String[] args) throws Exception {
        SparkSession sp = SparkSession
            .builder()
            .appName("TPE Grupo 1")
            .getOrCreate();
        
        GraphFrame myGraph = loadGraph(sp);
        long t1 = System.currentTimeMillis();

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
                                   .agg(avg(unix_timestamp(calls.col("l.endTime")).$minus(unix_timestamp(calls.col("l.startTime")))).as("duracion"));
        result.show();
        System.out.println("Took: " + (System.currentTimeMillis() - t1));
        sp.close();
    }
}
