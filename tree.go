package main

type Node struct {
	Order int
	Values []int
	Keys [][]int
	NextKey *Node
	Parent *Node
	IsLeaf bool
}

func (n *Node) insert_at_leaf(value, key int) {
	if (len(n.Values) > 0) {
		for i := 0; i < len(n.Values); i++ {
			if value == n.Values[i] {
				n.Keys[i] = append(n.Keys[i], key)
				return 
			} else if value < n.Values[i] {
				n.Values = append(n.Values[:i], append([]int{value}, n.Values[i:]...)...)
				n.Keys = append(n.Keys[:i], append([][]int{{key}}, n.Keys[i:]...)...)
				return
			} else {
				n.Values = append(n.Values, value)
				n.Keys = append(n.Keys, []int{key})
				return
			}
		}
	} else {
		n.Values = []int{value}
		n.Keys = [][]int{{key}}
	}
}

type BPlusTree struct {
	Root *Node
	Order int
}