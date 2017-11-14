package testing;

import java.util.ArrayList;
import java.util.List;
import java.sql.Timestamp;
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

public class GraphFramesSetup {
    private static StructType buildVertexSchema() {
        List<StructField> vertexFields = new ArrayList<StructField>();
        vertexFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
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

    private static Dataset<Row> populateVertices(JavaSparkContext JSPContext) {
        StructType vertexSchema = buildVertexSchema();
        ArrayList<Row> vertList = new ArrayList<Row>();
         
        vertList.add(RowFactory.create(1L, null, null, "4523-4132", null, "telefono"));
        vertList.add(RowFactory.create(2L, null, null, "5341-1231", null, "telefono"));
        vertList.add(RowFactory.create(3L, null, null, null, null, "usuario"));
        vertList.add(RowFactory.create(4L, null, null, null, null, "usuario"));
        vertList.add(RowFactory.create(5L, null, null, null, "CABA", "ciudad"));
        vertList.add(RowFactory.create(6L, null, null, null, "Argentina", "pais"));
        vertList.add(RowFactory.create(7L,
                                       Timestamp.valueOf("2017-10-10 10:15:34"), 
                                       Timestamp.valueOf("2017-10-10 10:19:20"),
                                       null,
                                       null,
                                       "llamada"));

        // association of both of them
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(JSPContext);
        Dataset<Row> verticesDF = sqlContext.createDataFrame(JSPContext.parallelize(vertList) , vertexSchema);

        return verticesDF;
    }

    private static Dataset<Row> populateEdges(JavaSparkContext JSPContext) {
        StructType edgeSchema = buildEdgeSchema();
        ArrayList<Row> edgeList = new ArrayList<Row>();
         
        edgeList.add(RowFactory.create(3L, 1L, "tiene_telefono"));
        edgeList.add(RowFactory.create(4L, 2L, "tiene_telefono"));
        edgeList.add(RowFactory.create(3L, 5L, "vive_en"));
        edgeList.add(RowFactory.create(4L, 5L, "vive_en"));
        edgeList.add(RowFactory.create(5L, 6L, "queda_en"));
        edgeList.add(RowFactory.create(1L, 7L, "creo"));
        edgeList.add(RowFactory.create(2L, 7L, "recibio"));

        // association of both of them
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(JSPContext);
        Dataset<Row> edgesDF = sqlContext.createDataFrame(JSPContext.parallelize(edgeList) , edgeSchema);

        return edgesDF;

    }

    public static void main(String[] args)
    {
        SparkSession sp = SparkSession
            .builder()
            .appName("TPE Grupo 1")
            .getOrCreate();
        JavaSparkContext JSPContext = new JavaSparkContext(sp.sparkContext());

        Dataset<Row> verticesDF = populateVertices(JSPContext);
        Dataset<Row> edgesDF = populateEdges(JSPContext);

        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);
        myGraph.vertices().createOrReplaceTempView("v_table");
        myGraph.edges().createOrReplaceTempView("v_edges");

        Dataset<Row> triplets = myGraph.find("(v1)-[e]->(v2)").
             filter("v2.type = 'llamada'");

        // from triplets to new edges
        Dataset<Row> auxiedges= triplets.select("e.src", "e.dst", "e.type");

        // filtering vertex
        Dataset<Row> auxivertices= myGraph.vertices();

        // build the graph
        GraphFrame lastothernewGraph = GraphFrame.apply(auxivertices, auxiedges);

        // in the driver
        System.out.println("myGraph:");
        lastothernewGraph.vertices().show();
        System.out.println();
        lastothernewGraph.edges().show();

        sp.close();
    }
}
