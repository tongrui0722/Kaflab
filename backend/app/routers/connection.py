"""Resolve managed-cluster credentials without returning them to browsers."""
from __future__ import annotations

from fastapi import HTTPException, Request

from .permissions import require_cluster


def managed_fields(request: Request, cluster_id: str) -> dict:
    require_cluster(request, cluster_id)
    cluster = request.app.state.clusters.get(cluster_id)
    if cluster is None:
        raise HTTPException(404, "cluster not found")
    return {
        "bootstrap": cluster.bootstrap,
        "securityProtocol": cluster.securityProtocol,
        "saslMechanism": cluster.saslMechanism,
        "saslUsername": cluster.saslUsername,
        "saslPassword": cluster.saslPassword,
    }
