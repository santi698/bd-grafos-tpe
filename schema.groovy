graph = JanusGraphFactory.open("conf/janusgraph-cassandra-es.properties");

mgmt = graph.openManagement();

llamada = mgmt.makeVertexLabel('llamada').make();

telefono = mgmt.makeVertexLabel('telefono').make();

cliente = mgmt.makeVertexLabel('cliente').make();

creo = mgmt.makeEdgeLabel('creo').multiplicity(Multiplicity.ONE2MANY).make();

participoEn = mgmt.makeEdgeLabel('participo_en').multiplicity(Multiplicity.MULTI).make();

tieneTelefono = mgmt.makeEdgeLabel('tiene_telefono').multiplicity(Multiplicity.ONE2MANY).make();

telefonoId = mgmt.makePropertyKey('telefono_id').dataType(Long.class).cardinality(Cardinality.SINGLE).make();

llamadaId = mgmt.makePropertyKey('llamada_id').dataType(Long.class).cardinality(Cardinality.SINGLE).make();

clientId = mgmt.makePropertyKey('client_id').dataType(Long.class).cardinality(Cardinality.SINGLE).make();

number = mgmt.makePropertyKey('number').dataType(String.class).cardinality(Cardinality.SINGLE).make();

city = mgmt.makePropertyKey('city').dataType(String.class).cardinality(Cardinality.SINGLE).make();

country = mgmt.makePropertyKey('country').dataType(String.class).cardinality(Cardinality.SINGLE).make();

company = mgmt.makePropertyKey('company').dataType(String.class).cardinality(Cardinality.SINGLE).make();

duracion = mgmt.makePropertyKey('duracion').dataType(Double.class).cardinality(Cardinality.SINGLE).make();

timestamp = mgmt.makePropertyKey('timestamp').dataType(String.class).cardinality(Cardinality.SINGLE).make();

mgmt.commit();
