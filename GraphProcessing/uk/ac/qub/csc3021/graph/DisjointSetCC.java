package uk.ac.qub.csc3021.graph;

import java.util.concurrent.atomic.AtomicIntegerArray;

// Calculate the connected components using disjoint set data structure
// This algorithm only works correctly for undirected graphs
public class DisjointSetCC {
	private static class DSCCRelax implements Relax {
		DSCCRelax(AtomicIntegerArray parent_) {
			// ...
			this.parent = parent_;
		}

		public void relax(int src, int dst) {
			union(src, dst);
		}

		//public int find(int x) {
			 //find method using path halving

			//if (parent.get(x) != x) {
				 //setting the parent of x to its grandparent
				//parent.set(x, find(parent.get(x)));
			//}

			//return parent.get(x);
		//}
		
		public int find(int x) {
			// find method using path compression

			int r = x;
			while(parent.get(r) != r) {
				parent.set(r, parent.get(parent.get(r)));
				r = parent.get(r);
			}

			return r;
		}
		

		private boolean sameSet(int x, int y) {
			// returns true if x and y in same set and false if not
			return find(x) == find(y);
		}

		private boolean union(int x, int y) { // link

			while (true) {
				// update parent pointers to grandparents
				x = find(x);
				y = find(y);
				if (x < y) {
					// update parent of x to y if x < y
					if (parent.compareAndSet(parent.get(x), x, y)) {
						return false;
					}
				}
				// return true if x and y in the same set already
				else if (sameSet(x, y)) {
					return true;
				}
				// updates parent of y to x if the root of the set with y is equal to y
				else if (parent.compareAndSet(parent.get(y), y, x)) {
					return false;
				}
			}
		}

		// Variable declarations
		private AtomicIntegerArray parent;
	};

	public static int[] compute(SparseMatrix matrix) {
		long tm_start = System.nanoTime();

		final int n = matrix.getNumVertices();
		final AtomicIntegerArray parent = new AtomicIntegerArray(n);
		final boolean verbose = true;

		for (int i = 0; i < n; ++i) {
			// Each vertex is a set on their own
			// ...
			parent.set(i, i);
		}

		DSCCRelax DSCCrelax = new DSCCRelax(parent);

		double tm_init = (double) (System.nanoTime() - tm_start) * 1e-9;
		System.err.println("Initialisation: " + tm_init + " seconds");
		tm_start = System.nanoTime();

		ParallelContext context = ParallelContextHolder.get();

		// 1. Make pass over graph
		context.ranged_edgemap(matrix, DSCCrelax);

		double tm_step = (double) (System.nanoTime() - tm_start) * 1e-9;
		if (verbose)
			System.err.println("processing time=" + tm_step + " seconds");
		tm_start = System.nanoTime();

		// Post-process the labels

		// 1. Count number of components
		// and map component IDs to narrow domain
		int ncc = 0;
		int remap[] = new int[n];
		for (int i = 0; i < n; ++i)
			if (DSCCrelax.find(i) == i)
				remap[i] = ncc++;

		if (verbose)
			System.err.println("Number of components: " + ncc);

		// 2. Calculate size of each component
		int sizes[] = new int[ncc];
		for (int i = 0; i < n; ++i)
			++sizes[remap[DSCCrelax.find(i)]];

		if (verbose)
			System.err.println("DisjointSetCC: " + ncc + " components");

		return sizes;
	}
}
