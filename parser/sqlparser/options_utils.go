package sqlparser

func injectEntry(v interface{}, entry OptionsSpecsEntry) interface{} {
	if len(entry.Key) == 0 {
		val, ok := entry.Value.(*SQLVal)
		if ok {
			return string(val.Val)
		}
		return entry.Value
	}
	if v == nil {
		v = map[string]interface{}{}
	}
	m := v.(map[string]interface{})
	if _, ok := m[entry.Key[0]]; !ok {
		m[entry.Key[0]] = map[string]interface{}{}
	}
	m[entry.Key[0]] = injectEntry(m[entry.Key[0]], OptionsSpecsEntry{
		Key:   entry.Key[1:],
		Value: entry.Value,
	})
	return m
}

func OptionsSpecsEntriesToMap(optsEntries []OptionsSpecsEntry) map[string]interface{} {
	var v interface{} = map[string]interface{}{}

	for _, entry := range optsEntries {
		// Reverse input key names
		for i, j := 0, len(entry.Key)-1; i < j; i, j = i+1, j-1 {
			entry.Key[i], entry.Key[j] = entry.Key[j], entry.Key[i]
		}
		v = injectEntry(v, entry)
	}

	return v.(map[string]interface{})
}