import numpy as np
import pytest

from common.similarity import build_embedding_matrix, top_n_similar


class TestBuildEmbeddingMatrix:
    def test_stacks_embeddings_into_matrix_and_computes_norms(self):
        embeddings = [[3.0, 4.0], [1.0, 0.0]]
        matrix, norms = build_embedding_matrix(embeddings)
        assert matrix.shape == (2, 2)
        assert matrix.dtype == np.float32
        np.testing.assert_allclose(norms, [5.0, 1.0])

    def test_norms_length_matches_number_of_rows(self):
        embeddings = [[1.0, 2.0, 3.0]] * 5
        matrix, norms = build_embedding_matrix(embeddings)
        assert matrix.shape[0] == 5
        assert norms.shape == (5,)


class TestTopNSimilar:
    def _matrix(self):
        # id "a": identical direction to "b" (similarity 1.0)
        # id "c": orthogonal to "a" (similarity 0.0)
        # id "d": opposite direction to "a" (similarity -1.0)
        ids = np.array(["a", "b", "c", "d"])
        embeddings = [
            [1.0, 0.0],
            [2.0, 0.0],   # same direction as a, different magnitude
            [0.0, 1.0],   # orthogonal to a
            [-1.0, 0.0],  # opposite of a
        ]
        matrix, norms = build_embedding_matrix(embeddings)
        return ids, matrix, norms

    def test_raises_value_error_when_target_id_not_found(self):
        ids, matrix, norms = self._matrix()
        with pytest.raises(ValueError, match="missing"):
            top_n_similar(ids, matrix, norms, "missing", top_n=2)

    def test_excludes_the_target_id_itself_from_results(self):
        ids, matrix, norms = self._matrix()
        rec_ids, _ = top_n_similar(ids, matrix, norms, "a", top_n=3)
        assert "a" not in rec_ids

    def test_orders_results_by_similarity_descending(self):
        ids, matrix, norms = self._matrix()
        rec_ids, rec_sims = top_n_similar(ids, matrix, norms, "a", top_n=3)
        # b (identical direction) most similar, then c (orthogonal), then d (opposite)
        assert list(rec_ids) == ["b", "c", "d"]
        assert rec_sims[0] > rec_sims[1] > rec_sims[2]

    def test_similarity_values_are_correct_cosine_similarity(self):
        ids, matrix, norms = self._matrix()
        rec_ids, rec_sims = top_n_similar(ids, matrix, norms, "a", top_n=3)
        sim_by_id = dict(zip(rec_ids, rec_sims))
        assert sim_by_id["b"] == pytest.approx(1.0, abs=1e-5)
        assert sim_by_id["c"] == pytest.approx(0.0, abs=1e-5)
        assert sim_by_id["d"] == pytest.approx(-1.0, abs=1e-5)

    def test_respects_top_n_limit(self):
        ids, matrix, norms = self._matrix()
        rec_ids, rec_sims = top_n_similar(ids, matrix, norms, "a", top_n=1)
        assert len(rec_ids) == 1
        assert rec_ids[0] == "b"

    def test_top_n_larger_than_available_candidates_returns_all_others(self):
        ids, matrix, norms = self._matrix()
        rec_ids, _ = top_n_similar(ids, matrix, norms, "a", top_n=100)
        assert len(rec_ids) == 3  # only 3 other ids exist besides "a"

    def test_works_with_string_and_integer_ids_alike(self):
        ids = np.array([1, 2, 3])
        matrix, norms = build_embedding_matrix([[1.0, 0.0], [1.0, 0.0], [0.0, 1.0]])
        rec_ids, _ = top_n_similar(ids, matrix, norms, 1, top_n=2)
        assert set(rec_ids) == {2, 3}
