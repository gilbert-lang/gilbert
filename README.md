gilbert
=======

Distributed Linear Algebra on Sparse Matrices

```Matlab

    % load network dataset
    numVertices = 50000000;
    N = load("hdfs://datasets/largenetwork.csv", numVertices, numVertices);

    % create the adjacency matrix 
    A = spones(N);

    % outdegree per vertex
    degrees = sum(A, 2);

    % create the column-stochastic transition matrix
    T = (diag(1 ./ degrees) * A)';

    % initialize the ranks
    r_0 = ones(numVertices, 1) / numVertices;

    % compute PageRank
    e = ones(numVertices, 1)
    beta = .85

    ranks = fixpoint(r_0, @(r) (beta * T * r + (1 - beta) * e));

    % save result
    write(ranks, "hdfs://results/pageranks.csv");
```
