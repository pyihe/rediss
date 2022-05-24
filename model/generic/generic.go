package generic

// MigrateOption 迁移key
type MigrateOption struct {
	Host        string
	Port        string
	Keys        []string
	Destination int
	Timeout     int64
	Copy        bool
	Replace     bool
	Username    string
	Password    string
}
