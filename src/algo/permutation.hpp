#pragma once

#include <random>
#include <algorithm>

struct Permutation {
	Permutation(const NodeId n, const NodeId a, const NodeId b) :
		n(n),
		log_n(static_cast<NodeId>(std::ceil(std::log2(n)))),
		mask(static_cast<NodeId>((1ull<<log_n) - 1)), a(a), b(b) {};

	Permutation(const NodeId n, std::mt19937_64& random_engine) : Permutation(n, 1, 0) {
		b = random_engine() & mask;
		a = ((random_engine() & (mask >> 1)) << 1) + 1;
	}

	Permutation() = default;

	Permutation invert() const {
		NodeId inv_a = 1;
		for (NodeId i = 1; ; ++i) {
			if (((i * a) & mask) == 1) {
				inv_a = i;
				break;
			}
		}

		NodeId inv_b = ((mask - b + 1) * inv_a) & mask;

		return Permutation(n, inv_a, inv_b);
	}

#pragma omp declare simd
	NodeId operator()(const NodeId u) const {
		return (a * u + b) & mask;
	}

	uint64_t max_input() const {
		return (1ull<<log_n);
	}


	NodeId n;
	NodeId log_n;
	NodeId mask;
	NodeId a;
	NodeId b;
};
