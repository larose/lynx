package query

import "fmt"

type MatchType byte

const (
	Should MatchType = iota
	Must
)

type BooleanClause struct {
	Type MatchType
	Node Node
}

type BooleanNode struct {
	Clauses []*BooleanClause
}

func (n *BooleanNode) CreateRootNode(context *QueryContext) (RootNode, error) {

	if len(n.Clauses) == 1 {
		return n.Clauses[0].Node.CreateRootNode(context)
	}

	childNodes := make([]ChildNode, 0, 10)

	allMust := true
	allShould := true

	for _, clause := range n.Clauses {
		allMust = allMust && clause.Type == Must
		allShould = allShould && clause.Type == Should

		childNode, err := clause.Node.CreateChildNode(context)
		if err != nil {
			return nil, err
		}

		childNodes = append(childNodes, childNode)
	}

	if allMust {
		return &ConjunctionRootNode{
			childNodes: childNodes,
		}, nil
	}

	if allShould {
		return &DisjunctionRootNode{
			childNodes: childNodes,
		}, nil
	}

	return nil, fmt.Errorf("must be either all should or all must")

}

func (n *BooleanNode) CreateChildNode(context *QueryContext) (ChildNode, error) {
	return nil, fmt.Errorf("not supported")
}
