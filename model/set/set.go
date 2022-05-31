package set

type ScanOption struct {
	Match string
	Count int64
}

type ScanResult struct {
	Cursor  int64
	Members []string
}
