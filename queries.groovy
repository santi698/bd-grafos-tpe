def shortestPathNaive(traversal, fromPhone, toPhone) {
  traversal.V().has('telefono_id', fromPhone).repeat(both().simplePath()).until(has('telefono_id', toPhone)).path().limit(1)
}

def shortestPath(traversal, fromPhone, toPhone) {
  traversal.
    V().
    has('telefono', 'telefono_id', fromPhone).
    as('source').
    outE('creo').
    inV().
    inE('participo_en').
    outV().
    until(
      has('telefono', 'telefono_id', toPhone)
    ).repeat(
      outE('creo', 'participo_en').
      inV().
      inE('creo', 'participo_en').
      outV().
      simplePath().
      where(neq('source'))
    ).
    path().
    by(coalesce(properties(), label())).
    limit(1);
}
