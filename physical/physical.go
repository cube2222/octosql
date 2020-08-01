package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/storage"
)

// Transformers is a structure containing functions to transform each of the physical plan components.
type Transformers struct {
	NodeT                             func(Node) Node
	ExprT                             func(Expression) Expression
	NamedExprT                        func(NamedExpression) NamedExpression
	FormulaT                          func(Formula) Formula
	TableValuedFunctionArgumentValueT func(TableValuedFunctionArgumentValue) TableValuedFunctionArgumentValue
	TriggerT                          func(Trigger) Trigger
	ShuffleStrategyT                  func(ShuffleStrategy) ShuffleStrategy
}

// MaterializationContext is a structure containing the configuration for the materialization.
type MaterializationContext struct {
	Config  *config.Config
	Storage storage.Storage
}

func NewMaterializationContext(config *config.Config, storage storage.Storage) *MaterializationContext {
	return &MaterializationContext{
		Config:  config,
		Storage: storage,
	}
}

type OutputOptions struct {
	OrderByExpressions []Expression
	OrderByDirections  []OrderDirection
	Limit              Expression
	Offset             Expression
}

func NewOutputOptions(
	orderByExpressions []Expression,
	orderByDirections []OrderDirection,
	limit Expression,
	offset Expression,
) *OutputOptions {
	return &OutputOptions{
		OrderByExpressions: orderByExpressions,
		OrderByDirections:  orderByDirections,
		Limit:              limit,
		Offset:             offset,
	}
}

func (opts *OutputOptions) Materialize(ctx context.Context, matCtx *MaterializationContext) (*execution.OutputOptions, error) {
	orderByExpressions := make([]execution.Expression, len(opts.OrderByExpressions))
	for i := range opts.OrderByExpressions {
		materializedExpr, err := opts.OrderByExpressions[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"couldn't materialize order by expression with index %d", i,
			)
		}

		orderByExpressions[i] = materializedExpr
	}

	orderByDirections := make([]execution.OrderDirection, len(opts.OrderByDirections))
	for i, dir := range opts.OrderByDirections {
		orderByDirections[i] = execution.OrderDirection(dir)
	}

	var limit execution.Expression
	if opts.Limit != nil {
		limitExpression, err := opts.Limit.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize limit expression")
		}
		limit = limitExpression
	}

	var offset execution.Expression
	if opts.Offset != nil {
		offsetExpression, err := opts.Offset.Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't materialize offset expression")
		}
		offset = offsetExpression
	}

	return execution.NewOutputOptions(orderByExpressions, orderByDirections, limit, offset), nil
}

// Node describes a single record stream source.
type Node interface {
	// Transform returns a new Node after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Node
	Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error)
	Metadata() *metadata.NodeMetadata
	graph.Visualizer
}

// Expressions describes a single value source.
type Expression interface {
	// Transform returns a new Expression after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Expression
	Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error)
	DoesMatchNamespace(namespace *metadata.Namespace) bool
	graph.Visualizer
}

// NamedExpressions describes a single named value source.
type NamedExpression interface {
	Expression
	// TransformNamed returns a new NamedExpression after recursively calling Transform
	ExpressionName() octosql.VariableName
	TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression
	MaterializeNamed(ctx context.Context, matCtx *MaterializationContext) (execution.NamedExpression, error)
}

type StarExpression struct {
	Qualifier string
}

func NewStarExpression(qualifier string) *StarExpression {
	return &StarExpression{Qualifier: qualifier}
}

func (se *StarExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	return se.TransformNamed(ctx, transformers)
}

func (se *StarExpression) ExpressionName() octosql.VariableName {
	if se.Qualifier == "" {
		return octosql.StarExpressionName
	}

	return octosql.NewVariableName(fmt.Sprintf("%s.%s", se.Qualifier, octosql.StarExpressionName))
}
func (se *StarExpression) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	return se.MaterializeNamed(ctx, matCtx)
}

func (se *StarExpression) TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression {
	var expr NamedExpression = &StarExpression{
		Qualifier: se.Qualifier,
	}

	if transformers.NamedExprT != nil {
		expr = transformers.NamedExprT(expr)
	}

	return expr
}

func (se *StarExpression) MaterializeNamed(ctx context.Context, matCtx *MaterializationContext) (execution.NamedExpression, error) {
	return execution.NewStarExpression(se.Qualifier), nil
}

func (se *StarExpression) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return true // doesn't really matter since a star expression won't appear in a Predicate
}

func (se *StarExpression) Visualize() *graph.Node {
	n := graph.NewNode("StarExpression")
	n.AddField("name", se.name())
	return n
}

func (se *StarExpression) name() string {
	if se.Qualifier == "" {
		return octosql.StarExpressionName
	}

	return fmt.Sprintf("%s_%s", se.Qualifier, octosql.StarExpressionName)
}

// Variables describes a variable name.
type Variable struct {
	Name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{Name: name}
}

func (v *Variable) Transform(ctx context.Context, transformers *Transformers) Expression {
	return v.TransformNamed(ctx, transformers)
}

func (v *Variable) ExpressionName() octosql.VariableName {
	return v.Name
}

func (v *Variable) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	return v.MaterializeNamed(ctx, matCtx)
}

func (v *Variable) TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression {
	var expr NamedExpression = &Variable{
		Name: v.Name,
	}
	if transformers.NamedExprT != nil {
		expr = transformers.NamedExprT(expr)
	}
	return expr
}

func (v *Variable) MaterializeNamed(ctx context.Context, matCtx *MaterializationContext) (execution.NamedExpression, error) {
	return execution.NewVariable(v.ExpressionName()), nil
}

func (v *Variable) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	if v.Name.Source() != "" {
		return namespace.DoesContainPrefix(v.Name.Source())
	}
	return true
}

func (v *Variable) Visualize() *graph.Node {
	n := graph.NewNode("Variable")
	n.AddField("name", v.ExpressionName().String())
	return n
}

// TupleExpression describes an expression which is a tuple of subexpressions.
type Tuple struct {
	Expressions []Expression
}

func NewTuple(expressions []Expression) *Tuple {
	return &Tuple{Expressions: expressions}
}

func (tup *Tuple) Transform(ctx context.Context, transformers *Transformers) Expression {
	exprs := make([]Expression, len(tup.Expressions))
	for i := range tup.Expressions {
		exprs[i] = tup.Expressions[i].Transform(ctx, transformers)
	}
	var transformed Expression = &Tuple{
		Expressions: exprs,
	}
	if transformers.ExprT != nil {
		transformed = transformers.ExprT(transformed)
	}
	return transformed
}

func (tup *Tuple) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	matExprs := make([]execution.Expression, len(tup.Expressions))
	for i := range tup.Expressions {
		materialized, err := tup.Expressions[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}
		matExprs[i] = materialized
	}

	return execution.NewTuple(matExprs), nil
}

func (tup *Tuple) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	for _, expr := range tup.Expressions {
		if !expr.DoesMatchNamespace(namespace) {
			return false
		}
	}
	return true
}

func (tup *Tuple) Visualize() *graph.Node {
	n := graph.NewNode("Tuple")
	for i := range tup.Expressions {
		n.AddChild(fmt.Sprintf("expr_%d", i), tup.Expressions[i].Visualize())
	}
	return n
}

// NodeExpressions describes an expression which gets it's value from a node underneath.
type NodeExpression struct {
	Node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{Node: node}
}

func (ne *NodeExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	var expr Expression = &NodeExpression{
		Node: ne.Node.Transform(ctx, transformers),
	}
	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (ne *NodeExpression) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	materialized, err := ne.Node.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize node")
	}
	return execution.NewNodeExpression(materialized, matCtx.Storage), nil
}

func (ne *NodeExpression) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return namespace.Contains(ne.Node.Metadata().Namespace())
}

func (ne *NodeExpression) Visualize() *graph.Node {
	n := graph.NewNode("Node Expression")
	n.AddChild("source", ne.Node.Visualize())
	return n
}

// LogicExpressions describes a boolean expression which get's it's value from the logic formula underneath.
type LogicExpression struct {
	Formula Formula
}

func NewLogicExpression(formula Formula) *LogicExpression {
	return &LogicExpression{Formula: formula}
}

func (le *LogicExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	var expr Expression = &LogicExpression{
		Formula: le.Formula.Transform(ctx, transformers),
	}
	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (le *LogicExpression) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	materialized, err := le.Formula.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize formula")
	}
	return execution.NewLogicExpression(materialized), nil
}

func (le *LogicExpression) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return le.Formula.DoesMatchNamespace(namespace)
}

func (le *LogicExpression) Visualize() *graph.Node {
	n := graph.NewNode("Logic Expression")
	n.AddChild("source", le.Formula.Visualize())
	return n
}

// AliasedExpression describes an expression which is explicitly named.
type AliasedExpression struct {
	ExpressionAlias octosql.VariableName
	Expr            Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) *AliasedExpression {
	return &AliasedExpression{ExpressionAlias: name, Expr: expr}
}

func (alExpr *AliasedExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	return alExpr.TransformNamed(ctx, transformers)
}

func (alExpr *AliasedExpression) ExpressionName() octosql.VariableName {
	return alExpr.ExpressionAlias
}

func (alExpr *AliasedExpression) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	return alExpr.MaterializeNamed(ctx, matCtx)
}

func (alExpr *AliasedExpression) TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression {
	var expr NamedExpression = &AliasedExpression{
		ExpressionAlias: alExpr.ExpressionName(),
		Expr:            alExpr.Expr.Transform(ctx, transformers),
	}
	if transformers.NamedExprT != nil {
		expr = transformers.NamedExprT(expr)
	}
	return expr
}

func (alExpr *AliasedExpression) MaterializeNamed(ctx context.Context, matCtx *MaterializationContext) (execution.NamedExpression, error) {
	materialized, err := alExpr.Expr.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize node")
	}
	return execution.NewAliasedExpression(alExpr.ExpressionName(), materialized), nil
}

func (alExpr *AliasedExpression) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return namespace.DoesContainName(alExpr.ExpressionName())
}

func (alExpr *AliasedExpression) Visualize() *graph.Node {
	n := graph.NewNode("Aliased Expression")
	n.AddField("alias", alExpr.ExpressionName().String())
	n.AddChild("expr", alExpr.Expr.Visualize())
	return n
}

type RecordExpression struct {
}

func NewRecordExpression() Expression {
	return &RecordExpression{}
}

func (r *RecordExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	var expr Expression = &RecordExpression{}
	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (r *RecordExpression) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Expression, error) {
	return execution.NewRecordExpression(), nil
}

func (r *RecordExpression) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return false
}

func (r *RecordExpression) Visualize() *graph.Node {
	n := graph.NewNode("Record Expression")
	return n
}
