package shardmaster

import (
	"math"
)

func rebalance(shards [NShards]int, gids []int) [NShards]int {
	//	fmt.Println("before", shards, gids)
	gset := make(map[int][]int)
	for _, v := range gids {
		gset[v] = []int{}
	}

	var list []int
	for i, v := range shards {
		if _, ok := gset[v]; ok {
			gset[v] = append(gset[v], i)
		} else {
			list = append(list, i)
		}
	}

	for _, v := range list {
		n := min(gset)
		gset[n] = append(gset[n], v)
	}

	for {
		n := max(gset)
		m := min(gset)
		if len(gset[n]) == len(gset[m]) || len(gset[n]) == len(gset[m])+1 {
			break
		} else {
			elem := gset[n][len(gset[n])-1]
			gset[n] = gset[n][:len(gset[n])-1]
			gset[m] = append(gset[m], elem)
		}
	}

	for k, v := range gset {
		for _, shard := range v {
			shards[shard] = k
		}
	}
	//	fmt.Println("after", shards, gids)
	return shards
}

func min(set map[int][]int) int {
	res := 0
	l := math.MaxInt32
	for k, v := range set {
		if len(v) < l {
			l = len(v)
			res = k
		}
	}
	return res
}

func max(set map[int][]int) int {
	res := 0
	l := math.MinInt32
	for k, v := range set {
		if len(v) > l {
			l = len(v)
			res = k
		}
	}
	return res
}
