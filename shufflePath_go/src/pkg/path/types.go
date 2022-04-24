package path

import "fmt"

type node int

// 节点的二元组 (v1,v2) 表示 v1指向v2的边
type edge struct {
	from node
	to   node
}

type edgeWeight struct {
	c float64 // 带宽 单位 MB/s
	d float64 // 距离 单位 m
	f float64 // 流量 单位 MB
}

type link map[edge]edgeWeight

type path []node

type pathEdge []edge

type nodeDirectPath map[edge]pathEdge

type nodeDirectInfo map[edge]float64

// 用于排序
type workerPathInfo struct {
	e          edge      //  (src, dst)
	loading    float64    // flow size
	p          path     // []edge
	idealDelay float64 // 独立最小时延
	delay      float64 // 真实时延
}

func (n node) equal(a node) bool {
	return n == a
}

func (e edge) String() string {
	return fmt.Sprintf("%v->%v", e.from, e.to)
}

func (e edge) reverse() edge {
	return edge{e.to, e.from}
}

func (l link) copy() link {
	res := make(link)
	for k, v := range l {
		res[k] = v
	}
	return res
}

func (p path) isContain(n node) bool {
	for _, nn := range p {
		if n.equal(nn) {
			return true
		}
	}
	return false
}

func (p path) toPathEdge() pathEdge {
	var pe pathEdge
	for i := 0; i < len(p)-1; i++ {
		pe = append(pe, edge{from: p[i], to: p[i+1]})
	}
	return pe
}

func (p path) copy() path {
	var res path
	for _, n := range p {
		res = append(res, n)
	}
	return res
}

func (p path) String() string {
	var str string
	for _, n := range p {
		str = fmt.Sprintf(str+"%v ", n)
	}
	return str[:len(str)-1]
}

func (pe pathEdge) toPath() path {
	var p path
	for i := 0; i < len(pe); i++ {
		p = append(p, pe[i].from)
	}
	p = append(p, pe[len(pe)-1].to)
	return p
}

func (wp nodeDirectPath) copy() nodeDirectPath {
	res := make(nodeDirectPath)
	for k, v := range wp {
		res[k] = v
	}
	return res
}

func (wpi workerPathInfo) String() string {
	return fmt.Sprintf("%v->%v idealDelay:%.1f realDelay:%.1f loading:%.1f path:%v",
		wpi.e.from, wpi.e.to, wpi.idealDelay, wpi.delay, wpi.loading, wpi.p)
}
