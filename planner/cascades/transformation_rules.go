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

package cascades

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/util/ranger"
)

// Transformation defines the interface for the transformation rules.
type Transformation interface {
	// ID returns the unique TransformationID of the transformation rule.
	ID() TransformationID
	// GetPattern creates the pattern of the transformation rule. All of the patterns should be
	// cached after initializing the package. So this method should never be called after that.
	GetPattern() *memo.Pattern
	// Match is used to check whether the GroupExpr satisfies all the requirements of the transformation rule.
	//
	// The pattern only identifies the operator type, some transformation rules also need
	// detailed information for certain plan operators to decide whether it is applicable.
	Match(expr *memo.ExprIter) bool
	// OnTransform does the real work of the optimization rule.
	//
	// newExprs indicates the new GroupExprs generated by the transformationrule. Multiple GroupExprs may be
	// returned, e.g, EnumeratePath would convert DataSource to several possible assess paths.
	//
	// eraseOld indicates that the returned GroupExpr must be better than the old one, so we can remove it from Group.
	//
	// eraseAll indicates that the returned GroupExpr must be better than all other candidates in the Group, e.g, we can
	// prune all other access paths if we found the filter is constantly false.
	OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error)
}

// TransformationID is the unique ID of a Transformation. When we want to add
// a new Transformation rule, we should first add its ID here, and create
// the rule in the defaultTransformationMap below.
type TransformationID int

const (
	rulePushSelDownTableScan TransformationID = iota
	rulePushSelDownTableGather
	rulePushSelDownSort
	rulePushSelDownProjection
	rulePushSelDownAggregation
	ruleEnumeratePaths
	rulePushAggDownGather
	ruleTransformLimitToTopN
	// ruleUnsupported is used for recording the number of rules.
	// It should always be the last TransformationID.
	ruleUnsupported
)

var defaultTransformationMap = map[memo.Operand][]Transformation{
	memo.OperandSelection: {
		&PushSelDownTableScan{},
		&PushSelDownTableGather{},
		&PushSelDownSort{},
		&PushSelDownProjection{},
		&PushSelDownAggregation{},
	},
	memo.OperandDataSource: {
		&EnumeratePaths{},
	},
	memo.OperandAggregation: {
		&PushAggDownGather{},
	},
	memo.OperandLimit: {
		&TransformLimitToTopN{},
	},
}

var patternMap []*memo.Pattern

// init initializes the patternMap when initializing the cascades package.
func init() {
	patternMap = make([]*memo.Pattern, ruleUnsupported)
	for _, rules := range defaultTransformationMap {
		for _, rule := range rules {
			patternMap[rule.ID()] = rule.GetPattern()
		}
	}
}

// GetPattern returns the Pattern of the given TransformationID.
func GetPattern(id TransformationID) *memo.Pattern {
	return patternMap[id]
}

// PushSelDownTableScan pushes the selection down to TableScan.
type PushSelDownTableScan struct {
}

// ID implements Transformation interface.
func (r *PushSelDownTableScan) ID() TransformationID {
	return rulePushSelDownTableScan
}

// GetPattern implements Transformation interface. The pattern of this rule is `Selection -> TableScan`.
func (r *PushSelDownTableScan) GetPattern() *memo.Pattern {
	ts := memo.NewPattern(memo.OperandTableScan, memo.EngineTiKVOrTiFlash)
	p := memo.BuildPattern(memo.OperandSelection, memo.EngineTiKVOrTiFlash, ts)
	return p
}

// Match implements Transformation interface.
func (r *PushSelDownTableScan) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
//
// It transforms `sel -> ts` to one of the following new exprs:
// 1. `newSel -> newTS`
// 2. `newTS`
//
// Filters of the old `sel` operator are removed if they are used to calculate
// the key ranges of the `ts` operator.
func (r *PushSelDownTableScan) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	ts := old.Children[0].GetExpr().ExprNode.(*plannercore.TableScan)
	if ts.Handle == nil {
		return nil, false, false, nil
	}
	accesses, remained := ranger.DetachCondsForColumn(ts.SCtx(), sel.Conditions, ts.Handle)
	if accesses == nil {
		return nil, false, false, nil
	}
	newTblScan := plannercore.TableScan{
		Source:      ts.Source,
		Handle:      ts.Handle,
		AccessConds: ts.AccessConds.Shallow(),
	}.Init(ts.SCtx(), ts.SelectBlockOffset())
	newTblScan.AccessConds = append(newTblScan.AccessConds, accesses...)
	tblScanExpr := memo.NewGroupExpr(newTblScan)
	if len(remained) == 0 {
		// `sel -> ts` is transformed to `newTS`.
		return []*memo.GroupExpr{tblScanExpr}, true, false, nil
	}
	schema := old.GetExpr().Group.Prop.Schema
	tblScanGroup := memo.NewGroupWithSchema(tblScanExpr, schema)
	newSel := plannercore.LogicalSelection{Conditions: remained}.Init(sel.SCtx(), sel.SelectBlockOffset())
	selExpr := memo.NewGroupExpr(newSel)
	selExpr.Children = append(selExpr.Children, tblScanGroup)
	// `sel -> ts` is transformed to `newSel ->newTS`.
	return []*memo.GroupExpr{selExpr}, true, false, nil
}

// PushSelDownTableGather pushes the selection down to child of TableGather.
type PushSelDownTableGather struct {
}

// ID implements Transformation interface.
func (r *PushSelDownTableGather) ID() TransformationID {
	return rulePushSelDownTableGather
}

// GetPattern implements Transformation interface. The pattern of this rule
// is `Selection -> TableGather -> Any`
func (r *PushSelDownTableGather) GetPattern() *memo.Pattern {
	any := memo.NewPattern(memo.OperandAny, memo.EngineTiKVOrTiFlash)
	tg := memo.BuildPattern(memo.OperandTableGather, memo.EngineTiDBOnly, any)
	p := memo.BuildPattern(memo.OperandSelection, memo.EngineTiDBOnly, tg)
	return p
}

// Match implements Transformation interface.
func (r *PushSelDownTableGather) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
//
// It transforms `oldSel -> oldTg -> any` to one of the following new exprs:
// 1. `newTg -> pushedSel -> any`
// 2. `remainedSel -> newTg -> pushedSel -> any`
func (r *PushSelDownTableGather) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	tg := old.Children[0].GetExpr().ExprNode.(*plannercore.TableGather)
	childGroup := old.Children[0].Children[0].Group
	var pushed, remained []expression.Expression
	sctx := tg.SCtx()
	_, pushed, remained = expression.ExpressionsToPB(sctx.GetSessionVars().StmtCtx, sel.Conditions, sctx.GetClient())
	if len(pushed) == 0 {
		return nil, false, false, nil
	}
	pushedSel := plannercore.LogicalSelection{Conditions: pushed}.Init(sctx, sel.SelectBlockOffset())
	pushedSelExpr := memo.NewGroupExpr(pushedSel)
	pushedSelExpr.Children = append(pushedSelExpr.Children, childGroup)
	pushedSelGroup := memo.NewGroupWithSchema(pushedSelExpr, childGroup.Prop.Schema).SetEngineType(childGroup.EngineType)
	// The field content of TableGather would not be modified currently, so we
	// just reference the same tg instead of making a copy of it.
	//
	// TODO: if we save pushed filters later in TableGather, in order to do partition
	//       pruning or skyline pruning, we need to make a copy of the TableGather here.
	tblGatherExpr := memo.NewGroupExpr(tg)
	tblGatherExpr.Children = append(tblGatherExpr.Children, pushedSelGroup)
	if len(remained) == 0 {
		// `oldSel -> oldTg -> any` is transformed to `newTg -> pushedSel -> any`.
		return []*memo.GroupExpr{tblGatherExpr}, true, false, nil
	}
	tblGatherGroup := memo.NewGroupWithSchema(tblGatherExpr, pushedSelGroup.Prop.Schema)
	remainedSel := plannercore.LogicalSelection{Conditions: remained}.Init(sel.SCtx(), sel.SelectBlockOffset())
	remainedSelExpr := memo.NewGroupExpr(remainedSel)
	remainedSelExpr.Children = append(remainedSelExpr.Children, tblGatherGroup)
	// `oldSel -> oldTg -> any` is transformed to `remainedSel -> newTg -> pushedSel -> any`.
	return []*memo.GroupExpr{remainedSelExpr}, true, false, nil
}

// EnumeratePaths converts DataSource to table scan and index scans.
type EnumeratePaths struct {
}

// ID implements Transformation interface.
func (r *EnumeratePaths) ID() TransformationID {
	return ruleEnumeratePaths
}

// GetPattern implements Transformation interface. The pattern of this rule is `DataSource`.
func (r *EnumeratePaths) GetPattern() *memo.Pattern {
	return memo.NewPattern(memo.OperandDataSource, memo.EngineTiDBOnly)
}

// Match implements Transformation interface.
func (r *EnumeratePaths) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
func (r *EnumeratePaths) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	ds := old.GetExpr().ExprNode.(*plannercore.DataSource)
	gathers := ds.Convert2Gathers()
	for _, gather := range gathers {
		expr := convert2GroupExpr(gather)
		expr.Children[0].SetEngineType(memo.EngineTiKV)
		newExprs = append(newExprs, expr)
	}
	return newExprs, true, false, nil
}

// PushAggDownGather splits Aggregation to two stages, final and partial1,
// and pushed the partial Aggregation down to the child of TableGather.
type PushAggDownGather struct {
}

// ID implements Transformation interface.
func (r *PushAggDownGather) ID() TransformationID {
	return rulePushAggDownGather
}

// GetPattern implements Transformation interface. The pattern of this rule
// is `Aggregation -> TableGather`.
func (r *PushAggDownGather) GetPattern() *memo.Pattern {
	return memo.BuildPattern(
		memo.OperandAggregation,
		memo.EngineTiDBOnly,
		memo.NewPattern(memo.OperandTableGather, memo.EngineTiDBOnly),
	)
}

// Match implements Transformation interface.
func (r *PushAggDownGather) Match(expr *memo.ExprIter) bool {
	agg := expr.GetExpr().ExprNode.(*plannercore.LogicalAggregation)
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.Mode != aggregation.CompleteMode {
			return false
		}
	}
	childEngine := expr.Children[0].GetExpr().Children[0].EngineType
	if childEngine != memo.EngineTiKV {
		// TODO: Remove this check when we have implemented TiFlashAggregation.
		return false
	}
	return plannercore.CheckAggCanPushCop(agg.SCtx(), agg.AggFuncs, agg.GroupByItems, false)
}

// OnTransform implements Transformation interface.
// It will transform `Agg->Gather` to `Agg(Final) -> Gather -> Agg(Partial1)`.
func (r *PushAggDownGather) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	agg := old.GetExpr().ExprNode.(*plannercore.LogicalAggregation)
	aggSchema := old.GetExpr().Group.Prop.Schema
	gather := old.Children[0].GetExpr().ExprNode.(*plannercore.TableGather)
	childGroup := old.Children[0].GetExpr().Children[0]
	// The old Aggregation should stay unchanged for other transformation.
	// So we build a new LogicalAggregation for the partialAgg.
	partialAggFuncs := make([]*aggregation.AggFuncDesc, len(agg.AggFuncs))
	for i, aggFunc := range agg.AggFuncs {
		newAggFunc := &aggregation.AggFuncDesc{
			HasDistinct: false,
			Mode:        aggregation.Partial1Mode,
		}
		newAggFunc.Name = aggFunc.Name
		newAggFunc.RetTp = aggFunc.RetTp
		// The args will be changed below, so that we have to build a new slice for it.
		newArgs := make([]expression.Expression, len(aggFunc.Args))
		copy(newArgs, aggFunc.Args)
		newAggFunc.Args = newArgs
		partialAggFuncs[i] = newAggFunc
	}
	partialGbyItems := make([]expression.Expression, len(agg.GroupByItems))
	copy(partialGbyItems, agg.GroupByItems)
	partialAgg := plannercore.LogicalAggregation{
		AggFuncs:     partialAggFuncs,
		GroupByItems: partialGbyItems,
	}.Init(agg.SCtx(), agg.SelectBlockOffset())
	partialAgg.CopyAggHints(agg)

	finalAggFuncs, finalGbyItems, partialSchema :=
		plannercore.BuildFinalModeAggregation(partialAgg.SCtx(), partialAgg.AggFuncs, partialAgg.GroupByItems, aggSchema)
	// Remove unnecessary FirstRow.
	partialAgg.AggFuncs =
		plannercore.RemoveUnnecessaryFirstRow(partialAgg.SCtx(), finalAggFuncs, finalGbyItems, partialAgg.AggFuncs, partialAgg.GroupByItems, partialSchema)
	finalAgg := plannercore.LogicalAggregation{
		AggFuncs:     finalAggFuncs,
		GroupByItems: finalGbyItems,
	}.Init(agg.SCtx(), agg.SelectBlockOffset())
	finalAgg.CopyAggHints(agg)

	partialAggExpr := memo.NewGroupExpr(partialAgg)
	partialAggExpr.SetChildren(childGroup)
	partialAggGroup := memo.NewGroupWithSchema(partialAggExpr, partialSchema).SetEngineType(childGroup.EngineType)
	gatherExpr := memo.NewGroupExpr(gather)
	gatherExpr.SetChildren(partialAggGroup)
	gatherGroup := memo.NewGroupWithSchema(gatherExpr, partialSchema)
	finalAggExpr := memo.NewGroupExpr(finalAgg)
	finalAggExpr.SetChildren(gatherGroup)
	// We don't erase the old complete mode Aggregation because
	// this transformation would not always be better.
	return []*memo.GroupExpr{finalAggExpr}, false, false, nil
}

// PushSelDownSort pushes the Selection down to the child of Sort.
type PushSelDownSort struct {
}

// ID implements Transformation interface.
func (r *PushSelDownSort) ID() TransformationID {
	return rulePushSelDownSort
}

// GetPattern implements Transformation interface. The pattern of this rule
// is `Selection -> Sort`.
func (r *PushSelDownSort) GetPattern() *memo.Pattern {
	return memo.BuildPattern(
		memo.OperandSelection,
		memo.EngineTiDBOnly,
		memo.NewPattern(memo.OperandSort, memo.EngineTiDBOnly),
	)
}

// Match implements Transformation interface.
func (r *PushSelDownSort) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
// It will transform `sel->sort->x` to `sort->sel->x`.
func (r *PushSelDownSort) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	sort := old.Children[0].GetExpr().ExprNode.(*plannercore.LogicalSort)
	childGroup := old.Children[0].GetExpr().Children[0]

	newSelExpr := memo.NewGroupExpr(sel)
	newSelExpr.Children = append(newSelExpr.Children, childGroup)
	newSelGroup := memo.NewGroupWithSchema(newSelExpr, childGroup.Prop.Schema)

	newSortExpr := memo.NewGroupExpr(sort)
	newSortExpr.Children = append(newSortExpr.Children, newSelGroup)
	return []*memo.GroupExpr{newSortExpr}, true, false, nil
}

// PushSelDownProjection pushes the Selection down to the child of Projection.
type PushSelDownProjection struct {
}

// ID implements Transformation interface.
func (r *PushSelDownProjection) ID() TransformationID {
	return rulePushSelDownProjection
}

// GetPattern implements Transformation interface.
func (r *PushSelDownProjection) GetPattern() *memo.Pattern {
	return memo.BuildPattern(
		memo.OperandSelection,
		memo.EngineTiDBOnly,
		memo.NewPattern(memo.OperandProjection, memo.EngineTiDBOnly),
	)
}

// Match implements Transformation interface.
func (r *PushSelDownProjection) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
// It will transform `selection -> projection -> x` to
// 1. `projection -> selection -> x` or
// 2. `selection -> projection -> selection -> x` or
// 3. just keep unchanged.
func (r *PushSelDownProjection) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	proj := old.Children[0].GetExpr().ExprNode.(*plannercore.LogicalProjection)
	childGroup := old.Children[0].GetExpr().Children[0]
	for _, expr := range proj.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			return nil, false, false, nil
		}
	}
	canBePushed := make([]expression.Expression, 0, len(sel.Conditions))
	canNotBePushed := make([]expression.Expression, 0, len(sel.Conditions))
	for _, cond := range sel.Conditions {
		if !expression.HasGetSetVarFunc(cond) {
			canBePushed = append(canBePushed, expression.ColumnSubstitute(cond, proj.Schema(), proj.Exprs))
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	if len(canBePushed) == 0 {
		return nil, false, false, nil
	}
	newBottomSel := plannercore.LogicalSelection{Conditions: canBePushed}.Init(sel.SCtx(), sel.SelectBlockOffset())
	newBottomSelExpr := memo.NewGroupExpr(newBottomSel)
	newBottomSelExpr.SetChildren(childGroup)
	newBottomSelGroup := memo.NewGroupWithSchema(newBottomSelExpr, childGroup.Prop.Schema)
	newProjExpr := memo.NewGroupExpr(proj)
	newProjExpr.SetChildren(newBottomSelGroup)
	if len(canNotBePushed) == 0 {
		return []*memo.GroupExpr{newProjExpr}, true, false, nil
	}
	newProjGroup := memo.NewGroupWithSchema(newProjExpr, proj.Schema())
	newTopSel := plannercore.LogicalSelection{Conditions: canNotBePushed}.Init(sel.SCtx(), sel.SelectBlockOffset())
	newTopSelExpr := memo.NewGroupExpr(newTopSel)
	newTopSelExpr.SetChildren(newProjGroup)
	return []*memo.GroupExpr{newTopSelExpr}, true, false, nil
}

// PushSelDownAggregation pushes Selection down to the child of Aggregation.
type PushSelDownAggregation struct {
}

// ID implements Transformation interface.
func (r *PushSelDownAggregation) ID() TransformationID {
	return rulePushSelDownAggregation
}

// GetPattern implements Transformation interface.
// The pattern of this rule is `Selection -> Aggregation`.
func (r *PushSelDownAggregation) GetPattern() *memo.Pattern {
	return memo.BuildPattern(
		memo.OperandSelection,
		memo.EngineAll,
		memo.NewPattern(memo.OperandAggregation, memo.EngineAll),
	)
}

// Match implements Transformation interface.
func (r *PushSelDownAggregation) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
// It will transform `sel->agg->x` to `agg->sel->x` or `sel->agg->sel->x`
// or just keep the selection unchanged.
func (r *PushSelDownAggregation) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	agg := old.Children[0].GetExpr().ExprNode.(*plannercore.LogicalAggregation)
	var pushedExprs []expression.Expression
	var remainedExprs []expression.Expression
	exprsOriginal := make([]expression.Expression, 0, len(agg.AggFuncs))
	for _, aggFunc := range agg.AggFuncs {
		exprsOriginal = append(exprsOriginal, aggFunc.Args[0])
	}
	groupByColumns := expression.NewSchema(agg.GetGroupByCols()...)
	for _, cond := range sel.Conditions {
		switch cond.(type) {
		case *expression.Constant:
			// Consider SQL list "select sum(b) from t group by a having 1=0". "1=0" is a constant predicate which should be
			// retained and pushed down at the same time. Because we will get a wrong query result that contains one column
			// with value 0 rather than an empty query result.
			pushedExprs = append(pushedExprs, cond)
			remainedExprs = append(remainedExprs, cond)
		case *expression.ScalarFunction:
			extractedCols := expression.ExtractColumns(cond)
			canPush := true
			for _, col := range extractedCols {
				if !groupByColumns.Contains(col) {
					canPush = false
					break
				}
			}
			if canPush {
				// TODO: Don't substitute since they should be the same column.
				newCond := expression.ColumnSubstitute(cond, agg.Schema(), exprsOriginal)
				pushedExprs = append(pushedExprs, newCond)
			} else {
				remainedExprs = append(remainedExprs, cond)
			}
		default:
			remainedExprs = append(remainedExprs, cond)
		}
	}
	// If no condition can be pushed, keep the selection unchanged.
	if len(pushedExprs) == 0 {
		return nil, false, false, nil
	}
	sctx := sel.SCtx()
	childGroup := old.Children[0].GetExpr().Children[0]
	pushedSel := plannercore.LogicalSelection{Conditions: pushedExprs}.Init(sctx, sel.SelectBlockOffset())
	pushedGroupExpr := memo.NewGroupExpr(pushedSel)
	pushedGroupExpr.SetChildren(childGroup)
	pushedGroup := memo.NewGroupWithSchema(pushedGroupExpr, childGroup.Prop.Schema)

	aggGroupExpr := memo.NewGroupExpr(agg)
	aggGroupExpr.SetChildren(pushedGroup)

	if len(remainedExprs) == 0 {
		return []*memo.GroupExpr{aggGroupExpr}, true, false, nil
	}

	aggGroup := memo.NewGroupWithSchema(aggGroupExpr, agg.Schema())
	remainedSel := plannercore.LogicalSelection{Conditions: remainedExprs}.Init(sctx, sel.SelectBlockOffset())
	remainedGroupExpr := memo.NewGroupExpr(remainedSel)
	remainedGroupExpr.SetChildren(aggGroup)
	return []*memo.GroupExpr{remainedGroupExpr}, true, false, nil
}

// TransformLimitToTopN transforms Limit+Sort to TopN.
type TransformLimitToTopN struct {
}

// ID implements Transformation interface.
func (r *TransformLimitToTopN) ID() TransformationID {
	return ruleTransformLimitToTopN
}

// GetPattern implements Transformation interface.
// The pattern of this rule is `Limit -> Sort`.
func (r *TransformLimitToTopN) GetPattern() *memo.Pattern {
	return memo.BuildPattern(
		memo.OperandLimit,
		memo.EngineTiDBOnly,
		memo.NewPattern(memo.OperandSort, memo.EngineTiDBOnly),
	)
}

// Match implements Transformation interface.
func (r *TransformLimitToTopN) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
// This rule will transform `Limit -> Sort -> x` to `TopN -> x`.
func (r *TransformLimitToTopN) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	limit := old.GetExpr().ExprNode.(*plannercore.LogicalLimit)
	sort := old.Children[0].GetExpr().ExprNode.(*plannercore.LogicalSort)
	childGroup := old.Children[0].GetExpr().Children[0]
	topN := plannercore.LogicalTopN{
		ByItems: sort.ByItems,
		Offset:  limit.Offset,
		Count:   limit.Count,
	}.Init(limit.SCtx(), limit.SelectBlockOffset())
	topNExpr := memo.NewGroupExpr(topN)
	topNExpr.SetChildren(childGroup)
	return []*memo.GroupExpr{topNExpr}, true, false, nil
}
