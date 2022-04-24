package path

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

// 全局变量
var (
	dimension = 4
	width     = dimension
	height    = dimension

	nodes      [][]node       // 存放棋盘格卫星
	linkOrigin link           // 初始的边的权值
	tasks      nodeDirectInfo // 存放卫星对任务量
	host      nodeDirectInfo  // 存放host连接数据量

	c []float64
	d []float64

	pathTmp []path
)

var once sync.Once // 单次锁

// todo
// 1 多个相同时延路线选最优
// 2 两点间最短距离

// Result todo 返回shuffle时间延及路径
func Result() {
	// 贪婪算法
	greedy2()
	// 最短距离算法
	minDistance()
}

func minDistance() {
	res := make(map[edge]workerPathInfo)
	matched := make(nodeDirectPath)

	// 遍历
	np := MinDelay(nil)
	for e, f := range tasks {
		idealDelay := np[e]
		p := minDistancePath(e)
		info := workerPathInfo{
			e:          e,
			p:          p,
			loading:    f,
			idealDelay: idealDelay,
		}
		res[e] = info
		matched[e] = p.toPathEdge()
	}

	// 增加与host的通信
	addHost(matched)

	// 结果处理
	maxDelay := 0.0
	for i, r := range res {
		delay := pathDelay(r.e, r.p.toPathEdge(), matched)
		tmp := res[i]
		tmp.delay = delay
		res[i] = tmp
		if delay > maxDelay {
			maxDelay = delay
		}
	}

	// print
	fmt.Println("============ 最短距离 result ===========")
	fmt.Printf("shuffle delay:%.2f\n", maxDelay)

	// data for python
	fmt.Println("== copy for python path.txt ==")
	for e, p := range matched {
		fmt.Printf("%v-%v:%v\n", e.from, e.to, p.toPath())
	}

}

func greedy() {
	res := make(map[edge]workerPathInfo)
	var maxDelayInfo workerPathInfo
	var wpi []workerPathInfo
	matched := make(nodeDirectPath)

	// 计算独立时延
step:
	np := MinDelay(matched)
	for e, f := range tasks {
		if _, ok := matched[e]; ok {
			continue
		}
		idealDelay := np[e]
		info := workerPathInfo{
			e:          e,
			loading:    f,
			idealDelay: idealDelay,
		}
		wpi = append(wpi, info)
	}

	// 对时延进行降序排列
	sort.SliceStable(wpi, func(i, j int) bool {
		return wpi[i].idealDelay > wpi[j].idealDelay
	})

	// 循环
	for i := range wpi {
		delay, p := minDelayPath(wpi[i].e, matched, maxDelayInfo.p)
		wpi[i].delay = delay
		wpi[i].p = p
		once.Do(func() { // 第一次成为最大
			maxDelayInfo = wpi[i]
		})
		matched[wpi[i].e] = p.toPathEdge()
		res[wpi[i].e] = wpi[i]
		combineDelay := minCombineDelay(matched)

		if combineDelay > maxDelayInfo.delay || delay > maxDelayInfo.delay {
			// 组合最优
			delete(matched, wpi[i].e)
			delay, p, maxDelayInfo = minCombineDelayPath(wpi[i].e, matched)
			wpi[i].delay = delay
			wpi[i].p = p
			matched[wpi[i].e] = p.toPathEdge()
			res[wpi[i].e] = wpi[i]
			wpi = make([]workerPathInfo, 0)
			goto step
		}
	}

	// 结果处理
	for i, r := range res {
		delay := pathDelay(r.e, r.p.toPathEdge(), matched)
		tmp := res[i]
		tmp.delay = delay
		res[i] = tmp
	}

	// print
	fmt.Println("============ result ===========")
	for _, r := range res {
		fmt.Println(r)
	}
	fmt.Println("============ max delay ===========")
	fmt.Printf("shuffle delay:%.2f\n", maxDelayInfo.delay)

	// data for python
	fmt.Println("============ copy for python path.txt ===========")
	for i, r := range res {
		fmt.Printf("%v-%v:%v\n", i.from, i.to, r.p)
	}

}

func greedy2() {
	res := make(map[edge]workerPathInfo)
	var wpi []workerPathInfo
	matched := make(nodeDirectPath)

	// 计算独立时延
	np := MinDelay(matched)
	for e, f := range tasks {
		idealDelay := np[e]
		info := workerPathInfo{
			e:          e,    // src, dst
			loading:    f,
			idealDelay: idealDelay,
		}
		wpi = append(wpi, info)
	}

	// 对时延进行降序排列
	sort.SliceStable(wpi, func(i, j int) bool {
		return wpi[i].idealDelay > wpi[j].idealDelay
	})

	// 遍历
	for i := range wpi {
		delay, p, _ := minCombineDelayPath(wpi[i].e, matched)
		wpi[i].delay = delay
		wpi[i].p = p
		matched[wpi[i].e] = p.toPathEdge()   // {(src, dst): paths}
		res[wpi[i].e] = wpi[i]
	}

	// 增加与host的通信
	addHost(matched)

	// 结果处理
	maxDelay := 0.0
	for i, r := range res {
		delay := pathDelay(r.e, r.p.toPathEdge(), matched)
		tmp := res[i]
		tmp.delay = delay
		res[i] = tmp
		if delay > maxDelay {
			maxDelay = delay
		}
	}

	// print
	fmt.Println("============ greedy算法 result ===========")
	fmt.Printf("shuffle delay:%.2f\n", maxDelay)

	// data for python
	fmt.Println("== copy for python path.txt ==")
	for e, p := range matched {
		fmt.Printf("%v-%v:%v\n", e.from, e.to, p.toPath())
	}

}

func addHost(matched nodeDirectPath ){
	for e := range host {
		p := minDistancePath(e)
		matched[e] = p.toPathEdge()
	}
}

// MinDelay 计算tasks中所有src到dst的最短时延距离（独立不干扰）
func MinDelay(matched nodeDirectPath) nodeDirectInfo {
	res := make(nodeDirectInfo)
	for e := range tasks {
		res[e] = NPsingle(e, matched)
	}
	return res
}

// NPsingle todo 修改算法,改为图中两点的最短距离
func NPsingle(direct edge, matched nodeDirectPath) float64 {
	max := 9999.0
	num := width * height
	dis := make([][]float64, num)
	for i := range dis {
		dis[i] = make([]float64, num)
		for j := range dis[i] {
			if i == j {
				dis[i][j] = 0
			} else {
				dis[i][j] = max
			}
		}
	}

	// 叠加背景流量 (已确定路径)
	linkTmp := linkOrigin.copy()
	if matched == nil || len(matched) == 0 {
		goto label
	}
	for wd, p := range matched {
		// nd 0 -> 1
		// p pathEdge
		for _, e := range p {
			l := linkTmp[e]
			l.f += tasks[wd]
			linkTmp[e] = l
			linkTmp[e.reverse()] = l
		}
	}

label:
	// 构建联结矩阵
	var l edgeWeight
	for _, row := range nodes {
		for _, src := range row {
			aroundNode := getAroundNode(src)
			for _, dst := range aroundNode {
				e := edge{from: src, to: dst}
				l = linkTmp[e]
				l.f = tasks[direct]
				dis[src][dst] = edgeDelay(l)

			}
		}
	}
	// 进行NP迭代
	var min float64
	for i := range dis {
		for j := range dis[i] {
			// 矩阵的最小和计算
			min = math.MaxFloat64
			for k := 0; k < num; k++ {
				d := dis[i][k] + dis[k][j]
				if d < min {
					min = d
				}
			}
			if min < dis[i][j] {
				dis[i][j] = min
			}
		}
	}
	return dis[direct.from][direct.to]
}

func pathDelay(curWD edge, curP pathEdge, matched nodeDirectPath) float64 {
	// 对已经匹配的路径和当前路径加入背景流量
	linkTmp := linkOrigin.copy()
	if matched == nil || len(matched) == 0 {
		matched = map[edge]pathEdge{curWD: curP}
	} else {
		matched[curWD] = curP
	}
	for wd, p := range matched {
		// e worker0 -> worker1
		// v pathEdge
		for _, e := range p {
			l := linkTmp[e]
			l.f += tasks[wd] + host[wd]
			linkTmp[e] = l
			linkTmp[e.reverse()] = l
		}
	}
	// 计算叠加了背景流量后当前路径时延
	var totalDelay float64
	for _, e := range curP {
		l := linkTmp[e]
		totalDelay += edgeDelay(l)
	}
	return totalDelay
}

func minDelayPath(e edge, matched nodeDirectPath, except path) (float64, path) {
	var minPATH path
	min := math.MaxFloat64
	var paths []path
	if except == nil || len(except) == 0 {
		paths = allPathByWD(e)
	} else {
		paths = allPathExceptByWD(e, except)
	}
	for _, p := range paths {
		delay := pathDelay(e, p.toPathEdge(), matched.copy())
		if delay < min {
			min = delay
			minPATH = p
		}
	}
	return min, minPATH
}

func minCombineDelay(matched nodeDirectPath) float64 {
	combineMaxDelay := 0.0
	mc := matched.copy()
	for mwd, pe := range mc {
		delay := pathDelay(mwd, pe, mc)
		if delay > combineMaxDelay {
			combineMaxDelay = delay
		}
	}
	return combineMaxDelay
}

func minCombineDelayPath(cur edge, matched nodeDirectPath) (float64, path, workerPathInfo) {
	var curPath, minPATH, combineMaxPATH path
	var minWD, combineMaxWD edge  // from to
	curDelay, minDelay, combineMaxDelay := 0.0, math.MaxFloat64, 0.0
	paths := allPathByWD(cur)
	for _, p := range paths {
		// 计算所有已经匹配的路径的时延
		mc := matched.copy()
		mc[cur] = p.toPathEdge()   // cur: src, dst
		combineMaxDelay = 0.0
		for mwd, pe := range mc {
			delay := pathDelay(mwd, pe, mc)  // (src, dst) paths
			if delay > combineMaxDelay {
				combineMaxDelay = delay
				combineMaxWD = mwd   // src dst
				combineMaxPATH = pe.toPath()
			}
		}
		if combineMaxDelay < minDelay {
			minDelay = combineMaxDelay
			minWD = combineMaxWD
			minPATH = combineMaxPATH
			curPath = p
			curDelay = pathDelay(cur, p.toPathEdge(), mc)
		}
	}
	return curDelay, curPath, workerPathInfo{e: minWD, p: minPATH, delay: minDelay}
}

func minDistancePath(e edge) path {
	var res path
	min := math.MaxInt32
	paths := allPathByWD(e)
	for i := range paths {
		if len(paths[i]) < min {
			min = len(paths[i])
			res = paths[i]
		}
	}
	return res
}

func allPathByWD(e edge) []path {
	return allPath(e.from, e.to)
}

func allPathExcept(s, t node, except path) []path {
	var res []path
	dfsExcept(s, t, []node{s}, except)
	// clone
	for i := range pathTmp {
		var tmp path
		for _, n := range pathTmp[i] {
			tmp = append(tmp, n)
		}
		res = append(res, tmp)
	}
	// return
	pathTmp = make([]path, 0)
	return res
}

func allPathExceptByWD(e edge, except path) []path {
	return allPathExcept(e.from, e.to, except)
}

func getAroundNode(n node) []node {
	p, q := node2Pos(n)
	var res []node
	var i, j int

	i, j = p-1, q
	if i >= 0 && i < height && j >= 0 && j < width {
		res = append(res, nodes[i][j])
	}
	i, j = p+1, q
	if i >= 0 && i < height && j >= 0 && j < width {
		res = append(res, nodes[i][j])
	}
	i, j = p, q-1
	if i >= 0 && i < height && j >= 0 && j < width {
		res = append(res, nodes[i][j])
	}
	i, j = p, q+1
	if i >= 0 && i < height && j >= 0 && j < width {
		res = append(res, nodes[i][j])
	}
	return res
}

func node2Pos(n node) (int, int) {
	id := int(n)
	return id / width, id % width
}

func edgeDelay(l edgeWeight) float64 {
	// timeSpread := l.d * 1000 / 3e8 // 传播时延：距离除以光速
	timeTrans := l.f / l.c // 传输时延： 数据量除以传输速率
	return timeTrans
}

func dfs(cur, t node, p path) {
	if cur.equal(t) {
		pathTmp = append(pathTmp, p.copy())
		return
	}
	// dfs
	aroundNode := getAroundNode(cur)
label:
	for _, n := range aroundNode {
		// check if cur is in the path
		for _, nn := range p {
			if n.equal(nn) {
				continue label
			}
		}
		newP := p.copy()
		dfs(n, t, append(newP, n))
	}
}

func dfsExcept(cur, t node, p, except path) {
	if cur.equal(t) {
		pathTmp = append(pathTmp, p.copy())
		return
	}
	// dfs
	aroundNode := getAroundNode(cur)
label:
	for _, n := range aroundNode {
		// check if cur is in the path
		if p.isContain(n) {
			continue label
		}
		if except.isContain(cur) && except.isContain(n) {
			continue label
		}
		newP := p.copy()
		dfsExcept(n, t, append(newP, n), except)
	}
}

// allPath s->t 所有可行路线
func allPath(s, t node) []path {
	var res []path
	dfs(s, t, []node{s})
	// clone
	for i := range pathTmp {
		var tmp path
		for _, n := range pathTmp[i] {
			tmp = append(tmp, n)
		}
		res = append(res, tmp)
	}
	// return
	pathTmp = make([]path, 0)
	return res
}
