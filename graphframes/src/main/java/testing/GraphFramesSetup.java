package testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import au.com.bytecode.opencsv.CSVReader;

import java.util.HashSet;

public class GraphFramesSetup {
    private static class SubGraph implements Serializable {
        private ArrayList<Row> vertices;
        private ArrayList<Row> edges;
        private StructType vertexSchema;
        private StructType edgeSchema;

        public SubGraph(StructType vertexSchema, StructType edgeSchema) {
            this.vertexSchema = vertexSchema;
            this.edgeSchema = edgeSchema;
            this.vertices = new ArrayList<Row>();
            this.edges = new ArrayList<Row>();
        }

        public void addVertex(Row vertex) {
            this.vertices.add(vertex);
        }

        public void addEdge(Row edge) {
            this.edges.add(edge);
        }

        public Dataset<Row> getVertices(JavaSparkContext context) {
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(context);
            Dataset<Row> df = sqlContext.createDataFrame(context.parallelize(this.vertices), this.vertexSchema);
            return df;
        }

        public Dataset<Row> getEdges(JavaSparkContext context) {
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(context);
            Dataset<Row> df = sqlContext.createDataFrame(context.parallelize(this.edges), this.edgeSchema);
            return df;
        }
    }
    private static class GlobalId implements Serializable {
        private static GlobalId instance = null;
        private long current = 0;
        protected GlobalId() {
           // Exists only to defeat instantiation.
        }
        public static GlobalId getInstance() {
           if(instance == null) {
              instance = new GlobalId();
           }
           return instance;
        }

        public long getNext() {
            return ++current;
        }
    }

    private static class LoadedUserIds implements Serializable {
        private static LoadedUserIds instance = null;
        private Set<Long> loadedIds;

        protected LoadedUserIds() {
            this.loadedIds = new HashSet<>();
        }

        public static LoadedUserIds getInstance() {
            if (instance == null) {
                instance = new LoadedUserIds();
            }

            return instance;
        }

        public void add(Long id) {
            loadedIds.add(id);
        }

        public boolean has(Long id) {
            return loadedIds.contains(id);
        }
    }
    private static StructType buildVertexSchema() {
        List<StructField> vertexFields = new ArrayList<StructField>();
        vertexFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertexFields.add(DataTypes.createStructField("typeId", DataTypes.LongType, true));
        vertexFields.add(DataTypes.createStructField("startTime", DataTypes.TimestampType, true));
        vertexFields.add(DataTypes.createStructField("endTime", DataTypes.TimestampType, true));
        vertexFields.add(DataTypes.createStructField("numero",DataTypes.StringType, true));
        vertexFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        vertexFields.add(DataTypes.createStructField("type",DataTypes.StringType, false));
        return DataTypes.createStructType(vertexFields);
    }

    private static StructType buildEdgeSchema() {
        List<StructField> edgeFields = new ArrayList<StructField>();
        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("type", DataTypes.StringType, false));
        return DataTypes.createStructType(edgeFields);
    }

    private static Long parseLong(Object o) {
        return Long.valueOf(((String) o).trim());
    }

    private static Timestamp parseTimestamp(Object string) {
        String str = (String) string;
        return Timestamp.valueOf(str.trim().substring(1, 20));
    }

    private static SubGraph buildTelefonos(List<Row> telefonos) {
        SubGraph subGraph = new SubGraph(buildVertexSchema(), buildEdgeSchema());
        telefonos.forEach((row) -> {
            if (LoadedUserIds.getInstance().has(parseLong(row.get(1)))) {
                Row user = RowFactory.create(GlobalId.getInstance().getNext(), parseLong(row.get(1)), null, null, null, null, "usuario");
                subGraph.addVertex(user);
                LoadedUserIds.getInstance().add(parseLong(row.get(1)));
            }
            Row telephone = RowFactory.create(GlobalId.getInstance().getNext(), parseLong(row.get(0)), null, null, row.get(2), null, "telefono");
            Row city = RowFactory.create(GlobalId.getInstance().getNext(), null, null, null, null, row.get(3), "ciudad");
            Row country = RowFactory.create(GlobalId.getInstance().getNext(), null, null, null, null, row.get(4), "pais");
            Row provider = RowFactory.create(GlobalId.getInstance().getNext(), null, null, null, null, row.get(5), "proveedor");
            Row userHasTelephone = RowFactory.create(parseLong(row.get(1)), parseLong(row.get(0)), "tiene_telefono");
            Row cityBelongsToCountry = RowFactory.create(city.get(0), country.get(0), "queda_en");
            Row userLivesIn = RowFactory.create(parseLong(row.get(1)), city.get(0), "vive_en");
            subGraph.addVertex(telephone);
            subGraph.addEdge(userHasTelephone);
            subGraph.addVertex(city);
            subGraph.addVertex(country);
            subGraph.addVertex(provider);
            subGraph.addEdge(cityBelongsToCountry);
            subGraph.addEdge(userLivesIn);
        });
        return subGraph;
    }

    private static SubGraph buildLlamadas(List<Row> llamadas) {
        SubGraph subGraph = new SubGraph(buildVertexSchema(), buildEdgeSchema());
        llamadas.forEach((row) -> {
            Row call = RowFactory.create(GlobalId.getInstance().getNext(), parseLong(row.get(0)), parseTimestamp(row.get(1)), parseTimestamp(row.get(2)), null, null, "llamada");
            Row userStartedCall = RowFactory.create(parseLong(row.get(3)), parseLong(row.get(0)), "creo");
            Row userWasInCall = RowFactory.create(parseLong(row.get(4)), parseLong(row.get(0)), "recibio");
            subGraph.addVertex(call);
            subGraph.addEdge(userWasInCall);
            subGraph.addEdge(userStartedCall);
        });
        return subGraph;
    }

    private static List<Row> readCSV(String location, SparkSession session) throws Exception {
        return session.read().csv(location).collectAsList();
    }

    public static void main(String[] args) throws Exception {
        SparkSession sp = SparkSession
            .builder()
            .appName("TPE Grupo 1")
            .getOrCreate();
        // Load data first time
        JavaSparkContext context = new JavaSparkContext(sp.sparkContext());
        
        List<Row> telefonos = readCSV("/user/socamica/telefonos_1M.csv", sp);
        SubGraph telefonosGraph = buildTelefonos(telefonos);
        List<Row> llamadas = readCSV("/user/socamica/llamadas_1M.csv", sp);
        SubGraph llamadasGraph = buildLlamadas(llamadas);

        Dataset<Row> verticesDF = telefonosGraph.getVertices(context).union(llamadasGraph.getVertices(context));
        Dataset<Row> edgesDF = telefonosGraph.getEdges(context).union(llamadasGraph.getEdges(context));

        // Persist data
        edgesDF.write().save("/user/socamica/grupo1v2-edges");
        verticesDF.write().save("/user/socamica/grupo1v2-vertices");

        // // Recover persisted data
        // Dataset<Row> edgesDF = sp.read().load("/user/socamica/grupo1v1-edges");
        // Dataset<Row> verticesDF = sp.read().load("/user/socamica/grupo1v1-vertices");
        
        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);
        System.out.println("myGraph:");
        myGraph.vertices().show();
        System.out.println();
        myGraph.edges().show();
        sp.close();
    }
}
