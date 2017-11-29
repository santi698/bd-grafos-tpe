package testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

public class SubGraph implements Serializable {
    public static final long serialVersionUID = 0l;
    private Dataset<Row> vertices;
    private Dataset<Row> edges;
    private StructType vertexSchema;
    private StructType edgeSchema;
    private SQLContext sqlContext;

    public SubGraph(StructType vertexSchema, StructType edgeSchema, JavaSparkContext context) {
        this.sqlContext = new SQLContext(context);
        this.vertexSchema = vertexSchema;
        this.edgeSchema = edgeSchema;
        this.vertices = sqlContext.createDataFrame(new ArrayList<Row>(), vertexSchema);
        this.edges = sqlContext.createDataFrame(new ArrayList<Row>(), edgeSchema);
    }

    public void addVertex(List<Row> vertices) {
        this.vertices = this.vertices.union(sqlContext.createDataFrame(vertices, vertexSchema));
    }

    public void addEdge(List<Row> edges) {
        this.edges = this.edges.union(sqlContext.createDataFrame(edges, edgeSchema));
    }

    public Dataset<Row> getVertices() {
        return this.vertices;
    }

    public Dataset<Row> getEdges() {
        return this.edges;
    }
}
