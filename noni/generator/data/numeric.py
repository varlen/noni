import math
import random

def get_random_interpolation_point(samples):
    position = random.random() * len(samples)
    index = math.floor(position)
    residue = position - index
    if index + 1 < len(samples):
        return (position, index, residue)
    else:
        return get_random_interpolation_point(samples)

def next_from_distribution(samples, as_integer = False):
    _, index, residue = get_random_interpolation_point(samples)
    next_value = samples[index] * (1 - residue) + samples[index + 1] * (residue)
    if as_integer:
        return round(next_value)
    return next_value
