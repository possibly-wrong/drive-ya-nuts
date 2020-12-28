"""Solving and counting Drive Ya Nuts puzzles.

A Drive Ya Nuts puzzle is a list of 7 hexagonal nuts, each identified by
a permutation of marks 0 through 5 arranged counterclockwise around its
edges. The nuts are arranged with puzzle[6] in the center, and
puzzle[0:6] in a ring counterclockwise around the center, with the k-th
edge of puzzle[6] facing the 0-th edge of puzzle[k].

The puzzle is solved when the nuts are permuted and rotated so that all
facing marks match, both with the center (puzzle[6][k] == puzzle[k][0])
and consecutively around the ring (puzzle[k][1] == puzzle[k - 1][-1]).
"""

from itertools import permutations, product, groupby
from multiprocessing import Pool
import numpy as np
import heapq
from collections import Counter

def align(nut, mark):
    """Return rotated nut so align(nut, mark)[0] == mark."""
    k = nut.index(mark)
    return nut[k:] + nut[:k]

def all_solutions(puzzle):
    """Generate all solutions of given puzzle."""

    # Start with each nut 0-aligned to eliminate trivial permutations of
    # duplicates.
    for nuts in set(permutations(align(nut, 0) for nut in puzzle)):
        ring = tuple(align(nut, mark) for mark, nut in zip(nuts[-1], nuts[:-1]))
        solved = True
        for k in range(6):
            if ring[k][1] != ring[k - 1][-1]:
                solved = False
                break
        if solved:
            yield ring + (nuts[-1],)

# Pre-compute mappings between puzzle nuts and "packed" integer indices.
packed = dict()
unpacked = dict()
for index, p in enumerate(permutations(range(1, 6))):
    packed[(0,) + p] = index
    unpacked[index] = (0,) + p

def pack(puzzle):
    """Convert puzzle to minimal integer representation."""
    return int.from_bytes(sorted(packed[align(nut, 0)] for nut in puzzle),
                          byteorder='big')

def unpack(index):
    """Convert integer back to puzzle."""
    return tuple(unpacked[nut] for nut in index.to_bytes(7, byteorder='big'))

def all_puzzles(center):
    """Generate all packed puzzles with given center nut."""

    # Find possible sequences of matching marks around the ring.
    for matching in product(range(6), repeat=6):
        halves = tuple(zip(matching, center, matching[-1:] + matching[:-1]))

        # If the result is valid, we can freely permute the remaining 3
        # outer marks on each nut around the ring.
        if all(len(set(inner)) == 3 for inner in halves):
            for ring in product(*([inner + outer
                                   for outer in permutations(
                                       set(range(6)) - set(inner))]
                                  for inner in halves)):
                yield pack(ring + (center,))

def save_puzzles(center):
    """Save sorted(all_puzzles(center)) to corresponding binary file."""
    filename = ''.join(str(mark) for mark in center)
    with open(filename, 'wb') as f:
        a = np.fromiter(all_puzzles(center), dtype=np.uint64)
        a.sort()
        a.tofile(f)

def load_puzzles(center):
    """Generate puzzles loaded from save_puzzles(center)."""
    filename = ''.join(str(mark) for mark in center)
    with open(filename, 'rb') as f:
        puzzle = f.read(8)
        while puzzle:
            yield int.from_bytes(puzzle, byteorder='little')
            puzzle = f.read(8)

if __name__ == '__main__':

    # Parallelize save_puzzles(.) for all possible center nuts.
    with Pool(6) as pool:
        for k, _ in enumerate(pool.imap_unordered(save_puzzles, packed)):
            print('{0} of {1} completed.'.format(k + 1, len(packed)))

    # Merge sort all files, counting distinct puzzles by multiplicity.
    counts = Counter(len(list(group)) for puzzle, group in groupby(
        heapq.merge(*(load_puzzles(center) for center in packed))))

    # Display results.
    for count in sorted(counts):
        print('{0:10} with {1:2} solutions.'.format(counts[count], count))
    print('{0:10} total unique puzzles.'.format(sum(counts.values())))

##    3899636160 with  1 solutions.
##     819638640 with  2 solutions.
##     169424400 with  3 solutions.
##      54822000 with  4 solutions.
##      14079840 with  5 solutions.
##       6587880 with  6 solutions.
##       2049120 with  7 solutions.
##        994080 with  8 solutions.
##        317520 with  9 solutions.
##        185760 with 10 solutions.
##         77760 with 11 solutions.
##         27240 with 12 solutions.
##         13680 with 13 solutions.
##          5040 with 14 solutions.
##          1440 with 15 solutions.
##          2160 with 16 solutions.
##          1800 with 20 solutions.
##    4967864520 total unique puzzles.

##Restrict to puzzles containing (0,1,2,3,4,5):
##     221013350 with  1 solutions.
##      46401870 with  2 solutions.
##       9627016 with  3 solutions.
##       3102349 with  4 solutions.
##        802208 with  5 solutions.
##        371888 with  6 solutions.
##        117102 with  7 solutions.
##         56392 with  8 solutions.
##         18030 with  9 solutions.
##         10512 with 10 solutions.
##          4452 with 11 solutions.
##          1565 with 12 solutions.
##           798 with 13 solutions.
##           282 with 14 solutions.
##            84 with 15 solutions.
##           120 with 16 solutions.
##            93 with 20 solutions.
##     281528111 total unique puzzles.
