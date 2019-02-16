def final1Naive(traversal, lbl, from, to) {
  traversal.V().has("${lbl}_id", from).repeat(both().simplePath()).until(has("${lbl}_id", to)).path().limit(1)
}

def final1(traversal, from, to) {
  traversal.
    V().
    has("client_id", from).
    repeat(
      outE('tiene_telefono').
      inV().
      outE('creo', 'participo_en').
      inV().
      inE('creo', 'participo_en').
      outV().
      inE('tiene_telefono').
      outV().
      simplePath()
    ).
    until(
      has("client_id", to)
    ).
    path().
    by(coalesce(properties(), label())).
    limit(1);
}

def final2a(traversal, fromUserId, toUserId) {
  final1(traversal, 'cliente', fromUserId, toUserId)
}

def final2b(traversal, fromUserId, toUserId) {
  // solo tiene sentido entre todo par de clientes
}

def final2c(traversal, fromUserId, toUserId) {
  // solo tiene sentido entre todo par de clientes
}

def final2d(traversal, from, distance) {
  traversal.V().has("client_id", from).repeat(out('tiene_telefono').out('creo', 'participo_en').in('creo', 'participo_en').in('tiene_telefono').simplePath()).times(distance).emit().path()
}
