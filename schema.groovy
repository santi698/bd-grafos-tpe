graph = JanusGraphFactory.open("/home/socamica/socamica.properties");

mgmt = graph.openManagement();

llamada = mgmt.makeVertexLabel('llamada').make();

telefono = mgmt.makeVertexLabel('telefono').make();

telefonoId = mgmt.makePropertyKey('telefonoId').dataType(Long.class).cardinality(Cardinality.SINGLE).make();
mgmt.buildIndex('byTelefonoId2', Vertex.class).addKey(telefonoId).buildCompositeIndex();

llamadaId = mgmt.makePropertyKey('llamadaId').dataType(Long.class).cardinality(Cardinality.SINGLE).make();
mgmt.buildIndex('byLlamadaId2', Vertex.class).addKey(llamadaId).buildCompositeIndex();

number = mgmt.makePropertyKey('number').dataType(String.class).cardinality(Cardinality.SINGLE).make();

city = mgmt.makePropertyKey('city').dataType(String.class).cardinality(Cardinality.SINGLE).make();

country = mgmt.makePropertyKey('country').dataType(String.class).cardinality(Cardinality.SINGLE).make();

company = mgmt.makePropertyKey('company').dataType(String.class).cardinality(Cardinality.SINGLE).make();

creo = mgmt.makeEdgeLabel('creo').multiplicity(Multiplicity.ONE2MANY).make();

participoEn = mgmt.makeEdgeLabel('participoEn').multiplicity(Multiplicity.MULTI).make();

duracion = mgmt.makePropertyKey('duracion2').dataType(Double.class).cardinality(Cardinality.SINGLE).make();

timestamp = mgmt.makePropertyKey('timestamp').dataType(Date.class).cardinality(Cardinality.SINGLE).make();
timestamp2 = mgmt.makePropertyKey('timestamp2').dataType(String.class).cardinality(Cardinality.SINGLE).make();

mgmt.commit();
