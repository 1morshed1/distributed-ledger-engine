package api

import (
	"github.com/1morshed1/distributed-ledger-engine/internal/util"
	"github.com/go-playground/validator/v10"
)

// validCurrency is a custom gin binding validator: `binding:"currency"`.
var validCurrency validator.Func = func(fl validator.FieldLevel) bool {
	if currency, ok := fl.Field().Interface().(string); ok {
		return util.IsSupportedCurrency(currency)
	}
	return false
}
