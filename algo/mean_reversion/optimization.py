import random, math
import numpy as np
from scipy.optimize import minimize


def generate_random_init(l, min_v, max_v):
    res = []
    for _ in range(l):
        res.append(min_v + random.randint(1, 100) * (max_v - min_v) / 100)
    rs = np.array(res)
    rs = rs / math.sqrt(np.sum(rs ** 2))
    return rs


def quadratic(mat, v):
    return mat @ np.array(v) @ np.array(v)

def quadratic_derivative(mat, v):
    return np.multiply(mat @ np.array(v), 2)

eq_cons = {'type': 'eq',
           'fun' : lambda x: np.array([np.sum(np.multiply(x, x)) - 1]),
           'jac' : lambda x: np.multiply(x, 2)}

def solve_min_quadratic(mat, iterations=3):
    min_v, max_v = -1, 1
    bounds = [(min_v, max_v) for _ in mat]

    v_min = None
    res = None
    for _ in range(iterations):
        x0 = generate_random_init(len(mat), min_v, max_v)
        res_min = minimize(lambda x: quadratic(mat, x), x0, method='SLSQP', jac=lambda x: quadratic_derivative(mat, x),
                       constraints=[eq_cons], options={'ftol': 1e-9, 'disp': False},
                       bounds=bounds)

        v = res_min.x @ mat @ res_min.x
        if v_min is None or v < v_min:
            v_min = v
            res = res_min.x

    return res, v_min