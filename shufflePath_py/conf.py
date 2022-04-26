
import random
import networkx as nx
import logging as log
from re import match
from matplotlib import pyplot as plt

# 全局变量
DIMENSION = 5
row = DIMENSION
col = DIMENSION
graph = nx.Graph(name = "sat topo")
nodes = [[(x * row + y) for x in range(row)] for y in range(col)]

def readAndCreate(): # -> list[list] nodes
    b = readBw()
    # tasks = readTask()

    nlist = [(x * row + y) for x in range(row) for y in range(col)]
    # print(nlist) 
    edges = []  #uv
    index = 0
    for x in range(row):
        for y in range(col - 1):
            edge = tuple()
            edge = (nodes[x][y], nodes[x][y + 1], {"bw": b[index], "volumes": 0})
            index += 1
            edges.append(edge)
    for x in range(row - 1):
        for y in range(col):
            edge = (nodes[x][y], nodes[x + 1][y], {"bw": b[index], "volumes": 0})
            index += 1
            edges.append(edge)
    
    graph.add_nodes_from(nlist)
    graph.add_edges_from(edges)
    # return nodes
        
    

def readBw() -> list:  
    # read bandwidth.txt  {b_uv}
    try:
        fileB = open("D:\\project\\py_project\\shufflePath\\bandwidth.txt", 'r')
    except:
        log.debug("os.open error")

    b = []
    while True:
        line = fileB.readline()
        if line == "":
            break
        for bw in map(int, line.split(" ")):
            b.append(bw) 
    fileB.close()
    return b


def readTask() -> list:
    # read task.txt {s_i, d_i, v_i}
    try:
        fileT = open("D:\\project\\py_project\\shufflePath\\task.txt", 'r')
    except:
        log.debug("os.open error")

    tasks = []
    tasks_map = {}
    while True:
        line = fileT.readline()
        if line == "":
            break
        if match(line, "#") is not None:
            continue
        task = list(map(int, line.split(" ")))
        tasks.append(task)
    fileT.close()
    for task in tasks:
        pair = (task[0], task[1])
        tasks_map[pair] = task[2]
    return tasks, tasks_map

def rand_task(n, workload):
    """
    this func generizes randomly flows of shuffle task. (e.g. coflow).  
     the num of workers is n, the whole workload size of shuffle flows is workload.
     We have assumed flow sizes are load balancing.
    """
    try:
        fileT = open("D:\\project\\py_project\\shufflePath\\task.txt", 'w')
    except:
        log.debug("os.open error")
    
    flow_size = workload / ((n-1)*n)
    flow_size = int(flow_size)
    length = row * col
    workers = set()
    while len(workers) < n:
        worker = random.randint(0, length - 1)
        workers.add(worker)

    pairs = []
    for i in workers:
        for j in workers:
            if i == j:
                continue
            else:
                pairs.append((i, j))
    
    for pair in pairs:
        line = str(pair[0]) + " " + str(pair[1]) + " " + str(flow_size)
        fileT.write(line + "\n")
    fileT.close()

def mapcopy(mymap: dict):
    newmap = {}
    for key in mymap:
        newmap[key] = mymap[key]
    return newmap

class Flow:
    def __init__(self, src, dst, volume, delay) -> None:
        self.src = src
        self.dst = dst
        self.volume = volume
        self.delay = delay

    def __repr__(self) -> str:
        return repr(self.src, self.dst, self.volume, self.volume)


'''
if __name__ == "__main__":
    readAndCreate()
    nx.draw(graph, with_labels=True, font_weight='bold')
    plt.show()
'''