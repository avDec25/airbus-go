package util

func IndexOf(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}

func Contains(a interface{}, list []interface{}) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
