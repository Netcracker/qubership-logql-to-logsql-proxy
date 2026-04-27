package parser

import (
	"testing"
	"time"
)

func TestInternalHelpersAndErrorStrings(t *testing.T) {
	var q Query = &LogQuery{}
	q.queryNode()
	var mq Query = &MetricQuery{Range: time.Minute}
	mq.queryNode()
	var aq Query = &AggregationQuery{}
	aq.queryNode()

	var stage PipelineStage = &LineFilter{}
	stage.stageNode()
	var labelStage PipelineStage = &LabelFilter{}
	labelStage.stageNode()
	var jsonStage PipelineStage = &JSONParser{}
	jsonStage.stageNode()
	var logfmtStage PipelineStage = &LogfmtParser{}
	logfmtStage.stageNode()

	if got := (&ParseError{Pos: 3, Msg: "bad token"}).Error(); got != "parse error at position 3: bad token" {
		t.Errorf("ParseError.Error() = %q", got)
	}
	if got := (&UnsupportedError{Pos: 7, Construct: "line_format"}).Error(); got != `unsupported LogQL construct "line_format" at position 7` {
		t.Errorf("UnsupportedError.Error() = %q", got)
	}
	if got := (token{val: "x", pos: 9}).String(); got != `"x"@9` {
		t.Errorf("token.String() = %q", got)
	}
	if got := tokenName(tokIDENT); got != "identifier" {
		t.Errorf("tokenName(tokIDENT) = %q", got)
	}
	if got := tokenName(tokenType(999)); got != "token(999)" {
		t.Errorf("tokenName(999) = %q", got)
	}
}
