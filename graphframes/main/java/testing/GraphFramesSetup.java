package testing;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.time.format.DateTimeFormatter;
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

public class GraphFramesSetup {
    private static StructType buildCallSchema() {
        List<StructField> callFields = new ArrayList<StructField>();
        callFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        callFields.add(DataTypes.createStructField("startTime", DataTypes.TimestampType, false));
        callFields.add(DataTypes.createStructField("endTime", DataTypes.TimestampType, false));
        // edge creador -> telefono
        // edge participante -> telefono

        return DataTypes.createStructType(callFields);
    }

    private static StructType buildTelephoneSchema() {
        List<StructField> telephoneFields = new ArrayList<StructField>();
        telephoneFields.add(DataTypes.createStructField("id",DataTypes.LongType, false));
        telephoneFields.add(DataTypes.createStructField("numero",DataTypes.StringType, false));
        telephoneFields.add(DataTypes.createStructField("usuario",DataTypes.StringType, false));
        // edge usuario -> usuario

        return DataTypes.createStructType(telephoneFields);
    }

    private static StructType buildUserSchema() {
        List<StructField> userFields = new ArrayList<StructField>();
        userFields.add(DataTypes.createStructField("id",DataTypes.LongType, false));
        // edge city -> City
        return DataTypes.createStructType(userFields);
    }

    private static StructType buildCitySchema() {
        List<StructField> cityFields = new ArrayList<StructField>();
        cityFields.add(DataTypes.createStructField("name",DataTypes.StringType, false));
        // edge country -> Country
        return DataTypes.createStructType(cityFields);
    }

    private static StructType buildCountrySchema() {
        List<StructField> countryFields = new ArrayList<StructField>();
        countryFields.add(DataTypes.createStructField("name",DataTypes.StringType, false));
        return DataTypes.createStructType(countryFields);
    }

    private static Dataset<Row> populateCallVertices(JavaSparkContext JSPContext, StructType callSchema) {
        ArrayList<Row> callList = new ArrayList<Row>();
        formatter = DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss Z");
        startTime = formatter.parse("2017-05-20 02:59:11 -0300");
        endTime = formatter.parse("2017-05-20 03:09:42 -0300");
        callList.add(RowFactory.create(1L, new Timestamp, "John"));

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(JSPContext);
        Dataset<Row> callVerticesDF = sqlContext.createDataFrame(JSPContext.parallelize(callList), callSchema);

        return callVerticesDF;
    }


    private static Dataset<Row> populateEdgesForGraphFrames(JavaSparkContext JSPContext)
    {

         // metadata
         List<StructField> edgeFields = new ArrayList<StructField>();
         edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, true));
         edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, true));
         edgeFields.add(DataTypes.createStructField("label",DataTypes.StringType, true));
         edgeFields.add(DataTypes.createStructField("count",DataTypes.IntegerType, true));

         StructType edgeSchema = DataTypes.createStructType(edgeFields);

         // values
         ArrayList<Row> edgeList = new ArrayList<Row>();

        edgeList.add(RowFactory.create(70L, 50L, "refersTo", 11) );
        edgeList.add(RowFactory.create(25L, 10L, "refersTo",  2) );
        edgeList.add(RowFactory.create(25L, 50L, "refersTo",  2) );
        edgeList.add(RowFactory.create(50L, 10L, "refersTo",  2) );
        edgeList.add(RowFactory.create(60L, 90L, "refersTo",  3) );
        edgeList.add(RowFactory.create(77L, 40L, "refersTo", 11) );
        edgeList.add(RowFactory.create(40L, 20L, "refersTo",  1) );
        edgeList.add(RowFactory.create(40L, 30L, "refersTo",  1) );
        edgeList.add(RowFactory.create(30L, 20L, "refersTo",  1) );
        edgeList.add(RowFactory.create(20L, 77L, "refersTo",  3) );

         // association of both of them
         SQLContext sqlContext = new org.apache.spark.sql.SQLContext(JSPContext);
         Dataset<Row> edgesDF = sqlContext.createDataFrame(JSPContext.parallelize(edgeList) , edgeSchema);

         return edgesDF;
    }


    public static void main(String[] args)
    {
        SparkSession sp = SparkSession
            .builder()
            .appName("GraphFrame TP08Ex5")
            .getOrCreate();
        JavaSparkContext JSPContext = new JavaSparkContext(sp.sparkContext());

        Dataset<Row> verticesDF = populateVerticesForGraphFrames(JSPContext);
        Dataset<Row> edgesDF = populateEdgesForGraphFrames(JSPContext);

        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);
        myGraph.vertices().createOrReplaceTempView("v_table");
        myGraph.edges().createOrReplaceTempView("v_edges");
        Dataset<Row> newVerticesDF= myGraph.sqlContext().sql("SELECT id, URL, owner, year(creationDate) as year from v_table");
        Dataset<Row> newEdgesDF= myGraph.sqlContext().sql("SELECT src, dst, label, case when count % 2 = 0 then false else true end as odd from v_edges");

        GraphFrame newGraph = GraphFrame.apply(newVerticesDF, newEdgesDF);

        // filtering edges through a pattern triplet condition
        Dataset<Row> triplets = newGraph.find("(v1)-[e]->(v2)").
             filter("v1.year = 2016 and e.odd = true and v2.year = 2016 ");

        // from triplets to new edges
        Dataset<Row> auxiedges= triplets.select("e.src", "e.dst", "e.label", "e.odd");

        // filtering vertex
        Dataset<Row> auxivertices= newGraph.vertices().filter("year = 2016");

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
