graph = JanusGraphFactory.open('conf/janusgraph-cassandra-es.properties');
graph.tx().rollback();

mgmt = graph.openManagement();

telefonoId = mgmt.getPropertyKey('telefono_id');
mgmt.buildIndex('byTelefonoId', Vertex.class).addKey(telefonoId).buildCompositeIndex();

llamadaId = mgmt.getPropertyKey('llamada_id');
mgmt.buildIndex('byLlamadaId', Vertex.class).addKey(llamadaId).buildCompositeIndex();

clientId = mgmt.getPropertyKey('client_id');
mgmt.buildIndex('byClientId', Vertex.class).addKey(clientId).buildCompositeIndex();

mgmt.commit();
