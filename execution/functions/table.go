package functions

import "github.com/cube2222/octosql/execution"

/* The only legal functions are the ones that appear in this
table. Otherwise the function will be considered undefined
and will throw an error on physical -> execution conversion.
IMPORTANT: As of now the lookup is case sensitive, so the functions must
be stored in lowercase, and the user must input them as lowercase as well.
*/
var FunctionTable = map[string]*execution.Function{}
var usableFunctions = []execution.Function{
	FuncInt,
	FuncFloat,
	FuncLower,
	FuncUpper,
	FuncNegate,
	FuncAbs,
	FuncCapitalize,
	FuncSqrt,
	FuncGreatest,
	FuncLeast,
	FuncRandInt,
	FuncRandFloat,
	FuncCeil,
	FuncFloor,
	FuncLog2,
	FuncLn,
	FuncPower,
	FuncReverse,
	FuncSubstring,
	FuncRegexpFind,
	FuncRegexpMatches,
	FuncNth,
	FuncReplace,
	FuncHasPrefix,
	FuncHasSuffix,
	FuncContains,
	FuncIndex,
	FuncLength,
	FuncNow,
	FuncStringJoin,
	FuncAdd,
	FuncSubtract,
	FuncMultiply,
	FuncDivide,
	FuncDuration,
	FuncCoalesce,
	FuncNullIf,
	FuncParseTime,
	FuncDecodeBase32,
}

func init() {
	for i := range usableFunctions {
		FunctionTable[usableFunctions[i].Name] = &usableFunctions[i]
	}
}
