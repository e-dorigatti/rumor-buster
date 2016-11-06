import numpy as np
import itertools


class AHClustering(object):
    def __init__(self, linkage, samples):
        self.linkage = linkage
        self.samples = samples
        self.sample_similarity_cache = {}
        self.cluster_similarity_cache = {}

    def sample_similarity(self, sample1, sample2):
        raise NotImplementedError

    def get_similarity(self, sample1, sample2):
        key = (sample1, sample2) if sample1 < sample2 else (sample2, sample1)
        if key not in self.sample_similarity_cache:
            self.sample_similarity_cache[key] = self.sample_similarity(
                self.samples[sample1], self.samples[sample2]
            )
        return self.sample_similarity_cache[key]

    def cluster_similarity(self, cluster1, cluster2):
        key = (cluster1, cluster2) if cluster1 < cluster2 else (cluster2, cluster2)
        if key not in self.cluster_similarity_cache:
            similarity = None
            for sample1, sample2 in itertools.product(cluster1, cluster2):
                sim = self.get_similarity(sample1, sample2)
                if self.linkage == 'single':
                    similarity = min(similarity, sim) if similarity else sim
                elif self.linkage == 'complete':
                    similarity = max(similarity, sim) if similarity else sim
                elif self.linkage == 'average':
                    if not similarity:
                        similarity = 0
                    similarity += sim
                else:
                    raise ValueError('unknown linkage: %s' % self.linkage)

            if self.linkage == 'average':
                similarity = float(similarity) / (len(cluster1) * len(cluster2))
            self.cluster_similarity_cache[key] = similarity
        return self.cluster_similarity_cache[key]

    def find_clusters(self, min_similarity):
        clusters = [(i,) for i in xrange(len(self.samples))]
        
        similarity = None
        while len(clusters) > 1:
            min_sim = merge = None
            for i in xrange(len(clusters)):
                for j in xrange(i + 1, len(clusters)):
                    sim = self.cluster_similarity(clusters[i], clusters[j])
                    if not min_sim or sim > min_sim:
                        min_sim = sim
                        merge = (i, j)

            similarity = sim
            if similarity > min_similarity:
                i, j = merge
                new_clusters = [c for k, c in enumerate(clusters) if k != i and k != j]
                new_clusters.append(tuple(sorted(clusters[i] + clusters[j])))
                clusters = new_clusters
            else:
                break

        return [[self.samples[s] for s in cluster] for cluster in clusters]


    def spark_find_clusters(self, sc, min_similarity):
        clusters = [(i,) for i in xrange(len(self.samples))]

        similarity = None
        while not similarity or similarity > min_similarity:
            clusters_rdd = sc.parallelize(enumerate(clusters))
            i, j, similarity = (clusters_rdd
                .cartesian(clusters_rdd)
                .map(lambda ((i, c1), (j, c2)): (i, j, self.cluster_similarity(c1, c2)))
                .reduce(lambda (i1, j1, s1), (i2, j2, s2): (
                    (i1, j1, s1) if s1 > s2 else (i2, j2, s2)
                ))
            )

            new_clusters = [c for k, c in enumerate(clusters) if k != i and k != j]
            new_clusters.append(tuple(sorted(clusters[i] + clusters[j])))
            clusters = new_clusters

        return clusters
