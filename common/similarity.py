"""
Shared cosine-similarity top-N logic, pure NumPy — no Spark/Streamlit
dependencies — so it's importable unchanged by both
notebooks/03_perfume_search.py (running inside Databricks) and
streamlit_app/app.py (running locally). Each caller supplies its own
(ids, embeddings) however it loaded them (Spark toPandas / SQL connector).
"""

import numpy as np


def build_embedding_matrix(embeddings) -> tuple:
    """embeddings: array-like of equal-length numeric vectors.
    Returns (matrix: float32 NxD array, norms: float32 N array)."""
    matrix = np.stack(embeddings).astype(np.float32)
    norms = np.linalg.norm(matrix, axis=1)
    return matrix, norms


def top_n_similar(ids, matrix, norms, target_id, top_n: int) -> tuple:
    """Cosine similarity of every row against target_id's row (a single
    vectorized matrix-vector product), top_n nearest excluding target_id
    itself. Returns (matched_ids, similarities). Raises ValueError if
    target_id has no row in `ids`."""
    matching = np.where(ids == target_id)[0]
    if len(matching) == 0:
        raise ValueError(f"id {target_id!r} not found in embeddings")
    target_idx = matching[0]
    similarities = (matrix @ matrix[target_idx]) / (norms * norms[target_idx])

    n = top_n + 1  # +1 to drop the target itself below
    top_idx = np.argpartition(-similarities, n - 1)[:n]
    top_idx = top_idx[np.argsort(-similarities[top_idx])]

    mask = ids[top_idx] != target_id
    return ids[top_idx][mask][:top_n], similarities[top_idx][mask][:top_n]
