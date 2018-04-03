package ingest

import (
	"testing"

	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/test"
)

func Test_ingestSignerEffects(t *testing.T) {
	tt := test.Start(t).ScenarioWithoutHorizon("set_options")
	defer tt.Finish()

	s := ingest(tt)
	tt.Require.NoError(s.Err)

	q := &history.Q{Session: tt.HorizonSession()}

	// Regression: https://github.com/stellar/horizon/issues/390 doesn't produce a signer effect when
	// inflation has changed
	var effects []history.Effect
	err := q.Effects().ForLedger(3).Select(&effects)
	tt.Require.NoError(err)

	if tt.Assert.Len(effects, 1) {
		tt.Assert.NotEqual(history.EffectSignerUpdated, effects[0].Type)
	}
}

func Test_ingestOperationEffects(t *testing.T) {
	tt := test.Start(t).ScenarioWithoutHorizon("set_options")
	defer tt.Finish()

	s := ingest(tt)
	tt.Require.NoError(s.Err)

	// ensure inflation destination change is correctly recorded
	q := &history.Q{Session: tt.HorizonSession()}
	var effects []history.Effect
	err := q.Effects().ForLedger(3).Select(&effects)
	tt.Require.NoError(err)

	if tt.Assert.Len(effects, 1) {
		tt.Assert.Equal(history.EffectAccountInflationDestinationUpdated, effects[0].Type)
	}
}

func Test_ingestPathPaymentExportsCorrectEffects(t *testing.T) {
	tt := test.Start(t).ScenarioWithoutHorizon("pathed_payment")
	defer tt.Finish()

	s := ingest(tt)
	tt.Require.NoError(s.Err)

	q := &history.Q{Session: tt.HorizonSession()}
	var effects []history.Effect
	err := q.Effects().ForOperation(25769807873).Select(&effects)
	tt.Require.NoError(err)

	if tt.Assert.Len(effects, 3) {
		tradeEffect := false
		accountCreditEffect := false
		accountDebitEffect := false

		for _, effect := range effects {
			if effect.Type == history.EffectTrade &&
				effect.Account == "GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON" {
				var details map[string]interface{}
				effect.UnmarshalDetails(&details)
				tt.Assert.Equal("GCXKG6RN4ONIEPCMNFB732A436Z5PNDSRLGWK7GBLCMQLIFO4S7EYWVU",
					details["seller"])
				tt.Assert.Equal("10.0000000", details["sold_amount"])
				tt.Assert.Equal("10.0000000", details["bought_amount"])
				tt.Assert.Equal("credit_alphanum4", details["sold_asset_type"])
				tt.Assert.Equal("GCQPYGH4K57XBDENKKX55KDTWOTK5WDWRQOH2LHEDX3EKVIQRLMESGBG",
					details["sold_asset_issuer"])
				tt.Assert.Equal("EUR", details["sold_asset_code"])
				tt.Assert.Equal("credit_alphanum4", details["bought_asset_type"])
				tt.Assert.Equal("GC23QF2HUE52AMXUFUH3AYJAXXGXXV2VHXYYR6EYXETPKDXZSAW67XO4",
					details["bought_asset_issuer"])
				tt.Assert.Equal("USD", details["bought_asset_code"])
				tradeEffect = true
			}
			if effect.Type == history.EffectAccountCredited {
				tt.Assert.Equal("GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2",
					effect.Account)
				var details map[string]interface{}
				effect.UnmarshalDetails(&details)
				tt.Assert.Equal("10.0000000", details["amount"])
				tt.Assert.Equal("credit_alphanum4", details["asset_type"])
				tt.Assert.Equal("GCQPYGH4K57XBDENKKX55KDTWOTK5WDWRQOH2LHEDX3EKVIQRLMESGBG",
					details["asset_issuer"])
				tt.Assert.Equal("EUR", details["asset_code"])
				accountCreditEffect = true
			}
			if effect.Type == history.EffectAccountDebited {
				tt.Assert.Equal("GCXKG6RN4ONIEPCMNFB732A436Z5PNDSRLGWK7GBLCMQLIFO4S7EYWVU",
					effect.Account)
				var details map[string]interface{}
				effect.UnmarshalDetails(&details)
				tt.Assert.Equal("10.0000000", details["amount"])
				tt.Assert.Equal("credit_alphanum4", details["asset_type"])
				tt.Assert.Equal("GC23QF2HUE52AMXUFUH3AYJAXXGXXV2VHXYYR6EYXETPKDXZSAW67XO4",
					details["asset_issuer"])
				tt.Assert.Equal("USD", details["asset_code"])
				accountDebitEffect = true
			}
		}
		tt.Assert.True(tradeEffect)
		tt.Assert.True(accountCreditEffect)
		tt.Assert.True(accountDebitEffect)
	}
}
