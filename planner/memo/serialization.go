// Copyright Mingcong Han.
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
	"fmt"
	"sort"
	"strings"
)

// MemoSnapshot records a snapshot of the memo.
// It is uses to trace the transformation of the memo.
type MemoSnapshot struct {
	// RuleName is the name of the transformation rule which is just applied to the memo.
	RuleName string `json:"rule_name"`
	// EraseOld marks if the old GroupExpr will be erase.
	EraseOld bool `json:"erase_old,string"`
	// EraseAll marks if all of the GroupExpr in this Group will be erase.
	EraseAll bool `json:"erase_all,string"`
	// Groups are the serialized memo structure.
	Groups []*GroupInfo `json:"groups"`
}

// GroupInfo is the serializable struct of Group.
type GroupInfo struct {
	// ID is used to identify a Group.
	ID int `json:"id"`
	// EngineType is the EngineType of the Group.
	EngineType string `json:"engine_type"`
	// Prop is the LogicalProperty of this Group.
	// Currently, it only contains the schema info of this Group.
	Prop string `json:"prop"`
	// Exprs are the equivalent GroupExprs in this Group.
	Exprs []*GroupExprInfo `json:"exprs"`
	// TODO: add Plan Implementations.
}

// GroupExprInfo is the serializable struct of GroupExpr.
type GroupExprInfo struct {
	// Operand is the LogicalPlan type of this GroupExpr.
	Operand string `json:"operand"`
	// ExplainID is an Operand with its PlanID.
	ExplainID string `json:"explain_id"`
	// ExprInfo is the explain info of this GroupExpr.
	ExprInfo string `json:"expr_info"`
	// Matched marks if this GroupExpr matches the pattern of a transformation rule.
	Matched bool `json:"matched"`
	// NewCreated marks if this GroupExpr is just created by a transformation rule.
	NewCreated bool `json:"new_created,string"`
	// ChildrenIDs are the IDs of the children Groups.
	ChildrenIDs []int `json:"children"`
	// Children are the children Groups of this GroupExpr.
	Children []*GroupInfo `json:"-"`
}

// SerializeMemo serializes a memo.
func SerializeMemo(g *Group, ruleName string, matchedExpr []*GroupExpr, newCreatedExprs []*GroupExpr, eraseOld, eraseAll bool) *MemoSnapshot {
	infoMap := make(map[*Group]*GroupInfo)
	serializeGroup(g, matchedExpr, newCreatedExprs, infoMap)
	groupInfos := make([]*GroupInfo, 0, len(infoMap))
	for _, info := range infoMap {
		groupInfos = append(groupInfos, info)
	}
	// Sort the slice by IDs.
	// We can make sure the first GroupInfo is always
	// the root Group.
	sort.Slice(groupInfos, func(i, j int) bool {
		return groupInfos[i].ID < groupInfos[j].ID
	})
	snapshot := &MemoSnapshot{
		Groups:   groupInfos,
		RuleName: ruleName,
		EraseOld: eraseOld,
		EraseAll: eraseAll,
	}
	return snapshot
}

func serializeGroup(g *Group, matchedExpr []*GroupExpr, newCreatedExprs []*GroupExpr, infoMap map[*Group]*GroupInfo) {
	if _, exists := infoMap[g]; exists {
		return
	}

	// Construct Property string.
	colStrs := make([]string, 0, len(g.Prop.Schema.Columns))
	for _, col := range g.Prop.Schema.Columns {
		colStrs = append(colStrs, col.String())
	}

	// Create a new GroupInfo for this Group.
	groupInfo := &GroupInfo{
		ID:         len(infoMap),
		EngineType: g.EngineType.String(),
		Prop:       fmt.Sprintf("Schema:[%s]", strings.Join(colStrs, ",")),
	}
	infoMap[g] = groupInfo

	// Construct GroupExprInfo.
	for item := g.Equivalents.Front(); item != nil; item = item.Next() {
		expr := item.Value.(*GroupExpr)
		exprInfo := serializeGroupExpr(expr, matchedExpr, newCreatedExprs, infoMap)
		groupInfo.Exprs = append(groupInfo.Exprs, exprInfo)
	}
}

func serializeGroupExpr(expr *GroupExpr, matchedExpr []*GroupExpr, newCreatedExprs []*GroupExpr, infoMap map[*Group]*GroupInfo) *GroupExprInfo {
	plan := expr.ExprNode
	exprInfo := &GroupExprInfo{
		Operand:  plan.TP(),
		ExprInfo: plan.ExplainInfo(),
	}
	// Check if this GroupExpr is matched by the rule.
	for _, matched := range matchedExpr {
		if matched == expr {
			exprInfo.Matched = true
		}
	}
	// Check if this GroupExpr is just created.
	for _, newExpr := range newCreatedExprs {
		if newExpr == expr {
			exprInfo.NewCreated = true
		}
	}
	for _, child := range expr.Children {
		serializeGroup(child, matchedExpr, newCreatedExprs, infoMap)
		exprInfo.ChildrenIDs = append(exprInfo.ChildrenIDs, infoMap[child].ID)
	}
	return exprInfo
}

// Flatten collects all of the GroupExpr inside a ExprIter.
func (iter *ExprIter) Flatten() []*GroupExpr {
	if iter.Element == nil {
		return nil
	}
	exprs := []*GroupExpr{iter.GetExpr()}
	for _, child := range iter.Children {
		exprs = append(exprs, child.Flatten()...)
	}
	return exprs
}
