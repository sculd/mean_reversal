import unittest
import sys
import numpy as np

class ItemTest(unittest.TestCase):

    def test_eigen_decomposition(self):
        mat = [[1, 2, 0], [2, 1, 0], [0, 0, 4]]
        eiven_vals, eigen_vecs = np.linalg.eig(mat)

        asc = np.argsort(eiven_vals)
        eiven_vals, eigen_vecs = eiven_vals[asc], eigen_vecs[:, asc]

        np.testing.assert_almost_equal(eiven_vals, np.array([ -1., 3., 4.]), decimal=3)
        np.testing.assert_almost_equal(eigen_vecs[:,0], np.array([-0.70710678, 0.70710678, 0.]), decimal=3)

        mat_reconstructed = eigen_vecs @ np.diag(eiven_vals) @ eigen_vecs.T
        np.testing.assert_almost_equal(mat_reconstructed, mat, decimal=3)


if __name__ == "__main__":
    unittest.main()
