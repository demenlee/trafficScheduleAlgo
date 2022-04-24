package path

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func ReadAndCreat() (err error) {
	err = read()
	if err != nil {
		return
	}

	// 初始化所有节点
	nodes = make([][]node, height)
	for i := range nodes {
		nodes[i] = make([]node, width)
		for j := range nodes[i] {
			nodes[i][j] = node(i*width + j)
		}
	}
	// 初始化所有横边
	index := 0
	linkOrigin = make(link)
	for i := 0; i < height; i++ {
		for j := 0; j < width-1; j++ {
			linkOrigin[edge{from: nodes[i][j], to: nodes[i][j+1]}] = edgeWeight{c: c[index], d: d[index]}
			index++
		}
	}
	// 初始化所有竖边
	for i := 0; i < height-1; i++ {
		for j := 0; j < width; j++ {
			linkOrigin[edge{from: nodes[i][j], to: nodes[i+1][j]}] = edgeWeight{c: c[index], d: d[index]}
		}
	}
	// 构成无向图
	for e, l := range linkOrigin {
		linkOrigin[e.reverse()] = l
	}
	return
}

func read() (err error) {
	if err = readBandAndDis();err!=nil{
		return
	}
	if err = readTaskAndHost();err!=nil{
		return
	}
	return
}

func readBandAndDis() (err error) {
	fileB, err := os.Open("conf/bandwidth.txt")
	fileD, err := os.Open("conf/distance.txt")
	if err != nil {
		fmt.Println("os.Open err", err)
		return
	}

	br := bufio.NewReader(fileB)
	for true {
		line, _, err := br.ReadLine()
		if err != nil {
			break
		}
		strs := strings.Split(string(line), " ")
		for _, str := range strs {
			float, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return fmt.Errorf("ParseFloat err %v", err)
			}
			c = append(c, float/8)
		}
	}

	br = bufio.NewReader(fileD)
	for true {
		line, _, err := br.ReadLine()
		if err != nil {
			break
		}
		strs := strings.Split(string(line), " ")
		for _, str := range strs {
			float, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return fmt.Errorf("ParseFloat err %v", err)
			}
			d = append(d, float)
		}
	}

	return
}

// todo task.txt的读取 正则匹配 或 改变为某种格式
func readTaskAndHost()(err error) {
	fileT, err := os.Open("conf/task.txt")
	fileH, err := os.Open("conf/host.txt")

	if err != nil {
		fmt.Println("os.Open err", err)
		return
	}

	tasks = make(nodeDirectInfo)
	host = make(nodeDirectInfo)

	br := bufio.NewReader(fileT)
	for true {
		line, _, err := br.ReadLine()
		if err != nil {
			break
		}
		// 省略注释行
		if strings.Contains(string(line),"#"){
			continue
		}
		if len(line)<4{
			continue
		}
		strs := strings.Split(string(line), " ")

		src, _ := strconv.Atoi(strs[0])
		dst, _ := strconv.Atoi(strs[1])
		e := edge{node(src), node(dst)}
		float, _ := strconv.ParseFloat(strs[2], 64)
		tasks[e] = float
	}

	br = bufio.NewReader(fileH)
	for true {
		line, _, err := br.ReadLine()
		if err != nil {
			break
		}
		// 省略注释行
		if strings.Contains(string(line),"#"){
			continue
		}
		if len(line)<4{
			continue
		}
		strs := strings.Split(string(line), " ")

		src, _ := strconv.Atoi(strs[0])
		dst, _ := strconv.Atoi(strs[1])
		e := edge{node(src), node(dst)}
		float, _ := strconv.ParseFloat(strs[2], 64)
		host[e] = float
	}
	return
}