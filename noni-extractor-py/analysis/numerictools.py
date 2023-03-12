import math
import random
import statistics

def get_single_sample(samples, integers = False):
    return sequence_from_samples(samples, 1, integers)

def sequence_from_samples(samples, length, integers = False, unique = False):
    """Returns a list with numbers having a similar distribution to the samples list."""
    samples.sort()
    output = []
    while len(output) < length:
        position = random.random() * len(samples)
        index = math.floor(position)
        residue = position - index
        if index + 1 < len(samples):
            next_value = samples[index] * (1 - residue) + samples[index + 1] * (residue)
            if integers:
                next_value = round(next_value)
            if unique and next_value in output:
                continue
            output.append(next_value)
    output.sort()
    return output

# new_sequence(k, 20, integers=True, unique=True)

# helpers

def get_gaussian_sequence(mu, sigma, length):
    return [ random.gauss(mu, sigma) for _ in range(length) ]

def gaussian_characteristics_variation():
    """ Check how mean and std variation changes when generating a sequence from the previous sequence
    """
    for _ in range(10):
        # 1 - Make original sequence
        mu = random.random() * 1000
        sigma = random.random() * 100
        gs = get_gaussian_sequence(mu, sigma, 20)
        # 2 - Make derived sequence
        ds = sequence_from_samples(gs, 20)
        # 3 - Get statistics
        gs_mu = statistics.mean(gs)
        ds_mu = statistics.mean(ds)
        gs_sigma = statistics.stdev(gs)
        ds_sigma = statistics.stdev(ds)
        print(f'mean: {gs_mu} -> {ds_mu} ; stdev: {gs_sigma} -> {ds_sigma}')


# TODO - Check "Original Sequence --> Sampled --> New Sequence" stats