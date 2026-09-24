package util

// Supported ISO 4217 currency codes. Kept small on purpose; extend as needed.
const (
	USD = "USD"
	EUR = "EUR"
	GBP = "GBP"
	CAD = "CAD"
	JPY = "JPY"
)

var supportedCurrencies = map[string]bool{
	USD: true, EUR: true, GBP: true, CAD: true, JPY: true,
}

// IsSupportedCurrency reports whether a currency code is one we handle.
func IsSupportedCurrency(code string) bool {
	return supportedCurrencies[code]
}
