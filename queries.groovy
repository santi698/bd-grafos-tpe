def allPaths(traversal, fromId, toId) {
  traversal.
    V().
    has('telefono', 'telefono_id', fromId).
    as('source').
    outE('creo').
    inV().
    inE('participo_en').
    outV().
    until(
      has('telefono', 'telefono_id', toId)
    ).repeat(
        outE('creo', 'participo_en').
        inV().
        inE('creo', 'participo_en').
        outV().
        simplePath().
        where(neq('source')
      )
    ).
    path().
    by(coalesce(properties(), label()));
}

def shortestPath(traversal, fromId, toId) {
  allPaths(traversal, fromId, toId).limit(1);
}
