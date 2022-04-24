

import re
import sys
from itertools import islice
from operator import attrgetter
import networkx as nx
import logging as log
import time

from pkg_resources import working_set

from conf import *
# tasks = list()
# tasks_map = dict()

def clear_graph_volumes():
    for x in range(row):
        for y in range(col - 1):
            u = nodes[x][y]
            v = nodes[x][y + 1]
            graph[u][v]["volumes"] = 0
    for x in range(row - 1):
        for y in range(col):
            u = nodes[x][y]
            v = nodes[x + 1][y]
            graph[u][v]["volumes"] = 0

def shuffle_time_shortest_path(tasks):  # -> float
    # readAndCreate()
    paths = addflowtoEdge_shortest_path(tasks)
    tasks_time = []
    for path in paths:
        time = 0
        for i in range(len(path) - 1):
            time += (8 * graph[path[i]][path[i+1]]["volumes"]) / graph[path[i]][path[i+1]]["bw"]
        tasks_time.append(time)
    # tasks_time.sort()
    res = max(tasks_time)
    # return tasks_time[len(tasks_time) - 1]
    return res

def addflowtoEdge_shortest_path(tasks: list) -> list:
    # tasks = readTask()
    paths = []
    for task in tasks:
        src = task[0]
        dst = task[1]
        volume = task[2]
        path = nx.shortest_path(graph, src, dst)
        paths.append(path)
        for i in range(len(path) - 1):
            graph[path[i]][path[i+1]]["volumes"] += volume
    return paths

def shuffle_time_ecmp(k, tasks):  # -> float
    # readAndCreate()
    paths = addflowtoEdge_ecmp(k, tasks).values()
    tasks_time = []
    for path_items in paths:
        task_time = []
        for path in path_items:
            time = 0
            for i in range(len(path) - 1):
                time += (8 * graph[path[i]][path[i+1]]["volumes"]) / graph[path[i]][path[i+1]]["bw"]
                task_time.append(time)
        time = max(task_time)
        tasks_time.append(time)
    # tasks_time.sort()
    # return tasks_time[len(tasks_time) - 1]
    return max(task_time)

def addflowtoEdge_ecmp(k: int, tasks: list) -> dict:
    # tasks = readTask()
    paths = {}
    for task in tasks:
        src = task[0]
        dst = task[1]
        volume = task[2]
        ec_volume = volume / k
        paths.setdefault((src, dst), list)
        gener = nx.shortest_simple_paths(graph, src, dst)
        paths_arr = list(islice(gener, k))
        
        paths[(src, dst)] = paths_arr
        for path in paths_arr:
            for i in range(len(path) - 1):
                graph[path[i]][path[i + 1]]["volumes"] += ec_volume
    return paths


def shuffle_time_greedSchedule(tasks, tasks_map) -> float:  
    """
        基于贪心实现的 单路径流量路径 调度路由算法。
    """
    flows, matched = singleDelayFlow(tasks)
    delay_map = dict()
    max_delay = 0.0
    for flow in flows:
        src = flow.src
        dst = flow.dst
        pair = (src, dst)
        flow_path, flow_delay = minCombineDelayPath(pair, matched, tasks_map)
        matched[pair] = flow_path
        delay_map[pair] = flow_delay
        if max_delay < flow_delay:
            max_delay = flow_delay
    return max_delay

def greedy(tasks, tasks_map) -> float:
    flows, matched = singleDelayFlow(tasks)
    delay_map = dict()
    max_delay = 0.0
    for flow in flows:
        src = flow.src
        dst = flow.dst
        pair = (src, dst)
        if len(delay_map) == 0:
            flow_delay, _ = combineMaxDelay(matched, tasks_map)
            delay_map[pair] = flow_delay
        else:
            flow_path, flow_delay = minCombineDelayPath(pair, matched, tasks_map)
            matched[pair] = flow_path
            delay_map[pair] = flow_delay
        if max_delay < flow_delay:
            max_delay = flow_delay
    return max_delay

#def minCombineDelayPath(flow: Flow, matched_path: dict):  
def minCombineDelayPath(pair: tuple, matched_path: dict, tasks_map):
    """
    调度选路: 迭代找出 每条路径的局部最优（从图中所有路径找一条路使得 整体delay 最小）
    """
    src = pair[0]
    dst = pair[1]
    gener = nx.shortest_simple_paths(graph, src, dst)
    #paths = list(gener)
    paths = list(islice(gener, 10))
    mindelay = sys.float_info.max
    minpair = tuple
    for p in paths:
        # matched_temp_path = matched_path.copy()
        matched_temp_path = mapcopy(matched_path)
        matched_temp_path[pair] = p
        cur_combinemax, cur_max_pair = combineMaxDelay(matched_temp_path, tasks_map)
        if mindelay > cur_combinemax:
            mindelay = cur_combinemax
            minpair = cur_max_pair
            curPath = p
            curDelay = flow_delay(p)
    return curPath, curDelay

def combineMaxDelay(temp_matched: dict, tasks_map: dict) -> tuple:
    combinemax = 0.0
    max_pair = tuple
    addflowtoEdge(temp_matched, tasks_map)
    for pair in temp_matched:
        temp_p = temp_matched[pair]
        delay = flow_delay(temp_p)
        if combinemax < delay:
            combinemax = delay
            max_pair = pair
    return combinemax, max_pair

def addflowtoEdge(matched: dict, tasks_map: dict):
    clear_graph_volumes()
    # print(matched)
    for pair in matched:
        flow_path = matched[pair]
        # print(pair)
        volume = tasks_map[pair]
        for i in range(len(flow_path) - 1):
            graph[flow_path[i]][flow_path[i+1]]["volumes"] += volume
    return 

def flow_delay(flow_path: list): # -> float
    path = flow_path
    time = 0
    for i in range(len(path) - 1):
        time += (8 * graph[path[i]][path[i+1]]["volumes"]) / graph[path[i]][path[i+1]]["bw"]
    return time


def singleDelayFlow(tasks) -> list:  # -> flows {src, dst, volume, delay}
    # clear_graph_volumes()
    #paths2volumes = []
    paths = []
    times = []
    flows = []
    matched = {}
    for task in tasks:
        src = task[0]
        dst = task[1]
        volume = task[2]
        pair = (src, dst)
        path = nx.shortest_path(graph, src, dst)
        matched[pair] = path
        #temp = (path, volume)
        paths.append(path)
        time = 0
        for i in range(len(path) - 1):
            time += (8 * volume) / graph[path[i]][path[i+1]]["bw"]
        times.append(time)
        flow = Flow(src, dst, volume, time)
        flows.append(flow)
    flows = sorted(flows, key=attrgetter('delay'))
    return flows, matched

def test(n: int):
    try:
        fileR = open("D:\\project\\py_project\\shufflePath\\res.txt", 'w')
    except:
        log.debug("os.open error")
    # print("------------------------------")
    # print("ECMP        SP       Greedy")
    line = "ECMP        SP       Greedy"
    fileR.write(line + "\n")
    for worker in range(4, 20):
        # print("num of workers %d", worker)
        line = "num of workers " + str(worker)
        fileR.write(line + "\n")
        for i in range(n):
            rand_task(worker, 5120)
            readAndCreate()
            tasks, tasks_map = readTask()
            shuffle_ecmp = shuffle_time_ecmp(6, tasks)
            clear_graph_volumes()
            shuffle_sp = shuffle_time_shortest_path(tasks)
            shuffle_greedy = shuffle_time_greedSchedule(tasks, tasks_map)
            # print("%0.3f        %0.3f       %0.3f" % (shuffle_ecmp, shuffle_sp, shuffle_greedy))
            line = "%0.3f        %0.3f       %0.3f" % (shuffle_ecmp, shuffle_sp, shuffle_greedy)
            fileR.write(line + "\n")
            graph.clear()
    fileR.close()

def test1(n: int):
    try:
        fileR = open("D:\\project\\py_project\\shufflePath\\res1.txt", 'w')
    except:
        log.debug("os.open error")
    line = "SP        ECMP       Greedy"
    fileR.write(line + "\n")
    coflow = 1024
    for size in range(1, 11):
        for i in range(n):
            rand_task(15, coflow * size)
            readAndCreate()
            tasks, tasks_map = readTask()
            shuffle_sp = shuffle_time_shortest_path(tasks)
            shuffle_ecmp = shuffle_time_ecmp(3, tasks)
            shuffle_greedy = shuffle_time_greedSchedule(tasks, tasks_map)
            line = "%0.3f        %0.3f       %0.3f" % (shuffle_sp, shuffle_ecmp, shuffle_greedy)
            fileR.write(line + "\n")
            graph.clear()
    fileR.close()

if __name__ == "__main__":
    test1(10)
    '''
    rand_task(16, 5112)
    readAndCreate()
    tasks, tasks_map = readTask()
    #nx.draw(graph, with_labels=True, font_weight='bold')
    #plt.show()
    time_start = time.time()
    shuffle_time1 = shuffle_time_shortest_path(tasks)
    time_end = time.time()
    print("shuffle time of ShortestPath route: %0.3f" %(shuffle_time1))
    print("process cost time: %0.3f" % (time_end - time_start))
    clear_graph_volumes()
    time_start = time.time()
    shuffle_time = shuffle_time_ecmp(5, tasks)
    time_end = time.time()
    print("shuffle time of ECMP route: %0.3f" %(shuffle_time))
    print("process cost time: %0.3f" % (time_end - time_start))
    time_start = time.time()
    shuffle_time2 = shuffle_time_greedSchedule(tasks, tasks_map)
    time_end = time.time()
    print("shuffle time of schedule route: %0.3f" %(shuffle_time2))
    print("process cost time: %0.3f" % (time_end - time_start))
    '''