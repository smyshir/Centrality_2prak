#import networkx as nx

#G = nx.Graph()
#counte = 0
#for i in open('EdgesAll.txt'):
#    counte += 1
#    node = list(map(int, i.split(',')))
#    G.add_edge(node[0], node[1])

#print("Центральность по степени")

#characters = sorted(list(nx.degree_centrality(G).items()), key=lambda i: i[1], reverse=True)
#print(*characters[:10], sep='\n')

#print("Центральность по посредничеству")
#characters = sorted(list(nx.betweenness_centrality(G).items()), key=lambda i: i[1], reverse=True)
#print(*characters[:10], sep='\n')

#print("Центральность по близости")
#characters = sorted(list(nx.closeness_centrality(G).items()), key=lambda i: i[1], reverse=True)
#print(*characters[:10], sep='\n')

####################################################################################

# import networkx as nx
# from joblib import Parallel, delayed
# from networkx.algorithms.centrality import betweenness_centrality, closeness_centrality
# import multiprocessing

# # Инициализация графа
# G = nx.Graph()

# # Загрузка рёбер в граф
# with open('EdgesAll.txt') as file:
#     edges = (tuple(map(int, line.split(','))) for line in file)
#     G.add_edges_from(edges)

# # Вычисление центральности по степени
# print("Центральность по степени")
# degree_centrality = nx.degree_centrality(G)
# top_degree = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
# print(*top_degree, sep='\n')

# # Параллельный расчет центральности по посредничеству
# def compute_betweenness(node):
#     # Рассчитываем центральность по посредничеству для одного узла
#     return (node, nx.betweenness_centrality_subset(G, [node], G.nodes(), normalized=True)[node])

# print("Центральность по посредничеству")
# num_cores = multiprocessing.cpu_count()
# betweenness_centrality = dict(Parallel(n_jobs=num_cores)(delayed(compute_betweenness)(node) for node in G.nodes()))
# top_betweenness = sorted(betweenness_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
# print(*top_betweenness, sep='\n')

# # Центральность по близости
# print("Центральность по близости")
# closeness_centrality = nx.closeness_centrality(G)
# top_closeness = sorted(closeness_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
# print(*top_closeness, sep='\n')

#########################################################################################

# import networkx as nx
# from joblib import Parallel, delayed
# import multiprocessing
# import heapq
#
# # Инициализация графа
# G = nx.Graph()
#
# # Загрузка рёбер в граф
# with open('EdgesAll.txt') as file:
#     edges = (tuple(map(int, line.strip().split(','))) for line in file)
#     G.add_edges_from(edges)
#
# # Вычисление центральности по степени
# print("Центральность по степени")
# degree_centrality = nx.degree_centrality(G)
# top_degree = heapq.nlargest(10, degree_centrality.items(), key=lambda x: x[1])
# print(*top_degree, sep='\n')
#
# # Полный расчет центральности по посредничеству
# print("\nЦентральность по посредничеству (полный расчет)")
#
# betweenness_centrality = nx.betweenness_centrality(G, normalized=True)
# top_betweenness = heapq.nlargest(10, betweenness_centrality.items(), key=lambda x: x[1])
# print(*top_betweenness, sep='\n')
#
# # Центральность по близости с параллельными вычислениями
# print("\nЦентральность по близости")
#
# def compute_closeness(node):
#     return (node, nx.closeness_centrality(G, u=node))
#
# num_cores = multiprocessing.cpu_count()
# closeness_centrality = dict(Parallel(n_jobs=num_cores)(delayed(compute_closeness)(node) for node in G.nodes()))
# top_closeness = heapq.nlargest(10, closeness_centrality.items(), key=lambda x: x[1])
# print(*top_closeness, sep='\n')

#######################################################################

import cudf
import cugraph

# Загрузка рёбер в DataFrame cudf
edges = []
with open('EdgesAll.txt') as file:
    for line in file:
        node1, node2 = map(int, line.strip().split(','))
        edges.append((node1, node2))

# Создаем DataFrame для работы с cuGraph
df = cudf.DataFrame(edges, columns=["src", "dst"])

# Создаем граф на GPU
G = cugraph.Graph()
G.from_cudf_edgelist(df, source="src", destination="dst", renumber=True)

# Центральность по степени
print("Центральность по степени")
degree_centrality = cugraph.degree_centrality(G)
print(degree_centrality.nlargest(10, 'degree_centrality'))

# Центральность по посредничеству
print("Центральность по посредничеству")
betweenness_centrality = cugraph.betweenness_centrality(G)
print(betweenness_centrality.nlargest(10, 'betweenness_centrality'))

# Центральность по близости
print("Центральность по близости")
closeness_centrality = cugraph.closeness_centrality(G)
print(closeness_centrality.nlargest(10, 'closeness_centrality'))
