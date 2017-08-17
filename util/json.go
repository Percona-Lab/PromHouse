package util

// MarshalLabels marshals Prometheus labels into JSON.
// It is significantly fast then json.Marshal.
// It is compatible with ClickHouse JSON functions: https://clickhouse.yandex/docs/en/functions/json_functions.html
func MarshalLabels(m map[string]string) []byte {
	b := make([]byte, 0, 128)
	b = append(b, '{')
	for k, v := range m {
		// add label name which can't contain runes that should be escaped
		b = append(b, '"')
		b = append(b, k...)
		b = append(b, '"', ':', '"')

		// add label value while escaping some runes
		for _, c := range []byte(v) {
			switch c {
			case '\\', '"':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, c)
			}
		}

		b = append(b, '"', ',')
	}

	b[len(b)-1] = '}'
	return b
}
