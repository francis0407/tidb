// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package memo

import (
	"container/list"
)

// ExprIter enumerates all the equivalent expressions in the Group according to
// the expression pattern.
type ExprIter struct {
	// Group and Element solely identify a Group expression.
	*Group
	*list.Element

	// matched indicates whether the current Group expression binded by the
	// iterator matches the pattern after the creation or iteration.
	matched bool

	// Operand and EngineTypeSet describe the node of pattern tree.
	// The Operand type and EngineTypeSet of the Group expression must be matched with it.
	Operand
	EngineTypeSet

	// Children is used to iterate the child expressions.
	Children []*ExprIter
}

// Next returns the next Group expression matches the pattern.
func (iter *ExprIter) Next() (found bool) {
	defer func() {
		iter.matched = found
	}()

	// Iterate child firstly.
	for i := len(iter.Children) - 1; i >= 0; i-- {
		if !iter.Children[i].Next() {
			continue
		}

		for j := i + 1; j < len(iter.Children); j++ {
			iter.Children[j].Reset()
		}
		return true
	}

	// It's root node or leaf ANY node.
	if iter.Group == nil || iter.Operand == OperandAny {
		return false
	}

	// Otherwise, iterate itself to find more matched equivalent expressions.
	for elem := iter.Element.Next(); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.ExprNode)

		if !iter.Operand.Match(exprOperand) {
			// All the Equivalents which have the same Operand are continuously
			// stored in the list. Once the current equivalent can not Match
			// the Operand, the rest can not, either.
			return false
		}

		if len(iter.Children) == 0 {
			iter.Element = elem
			return true
		}
		if len(iter.Children) != len(expr.Children) {
			continue
		}

		allMatched := true
		for i := range iter.Children {
			iter.Children[i].Group = expr.Children[i]
			if !iter.Children[i].Reset() {
				allMatched = false
				break
			}
		}

		if allMatched {
			iter.Element = elem
			return true
		}
	}
	return false
}

// Matched returns whether the iterator founds a Group expression matches the
// pattern.
func (iter *ExprIter) Matched() bool {
	return iter.matched
}

// Reset resets the iterator to the first matched Group expression.
func (iter *ExprIter) Reset() (findMatch bool) {
	defer func() { iter.matched = findMatch }()

	if !iter.EngineTypeSet.Contain(iter.Group.EngineType) {
		return false
	}

	if iter.Operand == OperandAny {
		return true
	}

	for elem := iter.Group.GetFirstElem(iter.Operand); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.ExprNode)
		if !iter.Operand.Match(exprOperand) {
			break
		}

		if len(iter.Children) == 0 {
			iter.Element = elem
			return true
		}
		if len(expr.Children) != len(iter.Children) {
			continue
		}

		allMatched := true
		for i := range iter.Children {
			iter.Children[i].Group = expr.Children[i]
			if !iter.Children[i].Reset() {
				allMatched = false
				break
			}
		}
		if allMatched {
			iter.Element = elem
			return true
		}
	}
	return false
}

// GetExpr returns the root GroupExpr of the iterator.
func (iter *ExprIter) GetExpr() *GroupExpr {
	return iter.Element.Value.(*GroupExpr)
}

// NewExprIterFromGroupElem creates the iterator on the Group Element.
func NewExprIterFromGroupElem(elem *list.Element, p *Pattern) *ExprIter {
	expr := elem.Value.(*GroupExpr)
	if !p.Operand.Match(GetOperand(expr.ExprNode)) || !p.EngineTypeSet.Contain(expr.Group.EngineType) {
		return nil
	}
	iter := newExprIterFromGroupExpr(expr, p)
	if iter != nil {
		iter.Element = elem
	}
	return iter
}

// newExprIterFromGroupExpr creates the iterator on the Group expression.
func newExprIterFromGroupExpr(expr *GroupExpr, p *Pattern) *ExprIter {
	if len(p.Children) != 0 && len(p.Children) != len(expr.Children) {
		return nil
	}
	iter := &ExprIter{Operand: p.Operand, EngineTypeSet: p.EngineTypeSet, matched: true}
	for i := range p.Children {
		childIter := newExprIterFromGroup(expr.Children[i], p.Children[i])
		if childIter == nil {
			return nil
		}
		iter.Children = append(iter.Children, childIter)
	}
	return iter
}

// newExprIterFromGroup creates the iterator on the Group.
func newExprIterFromGroup(g *Group, p *Pattern) *ExprIter {
	if !p.EngineTypeSet.Contain(g.EngineType) {
		return nil
	}
	if p.Operand == OperandAny {
		return &ExprIter{Group: g, Operand: OperandAny, EngineTypeSet: p.EngineTypeSet, matched: true}
	}
	for elem := g.GetFirstElem(p.Operand); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpr)
		if !p.Operand.Match(GetOperand(expr.ExprNode)) {
			return nil
		}
		iter := newExprIterFromGroupExpr(expr, p)
		if iter != nil {
			iter.Group, iter.Element = g, elem
			return iter
		}
	}
	return nil
}
