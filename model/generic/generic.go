package generic

// MigrateOption 迁移key
type MigrateOption struct {
	Host        string   // 目标Redis服务器主机
	Port        string   // 目标Redis服务器端口
	Keys        []string // 需要迁移的key
	Destination int      // 目标数据库
	Timeout     int64    // 超时时间, 单位毫秒
	Copy        bool     // 是否需要从源实例中删除
	Replace     bool     // 是否替换远端服务器上的key
	Username    string   // 用户名
	Password    string   // 密码
}

// RestoreOption RESTORE 命令选项
type RestoreOption struct {
	TTL      int64 //key有效期, 单位毫秒, 如果指定了ABSTTL, 则TTL为精确到毫秒的时间戳
	Replace  bool  // 是否替换已经存在的key
	ABSTTL   bool  // 有效期时间是否使用毫秒级时间戳
	IdleTime int64 // IDLETIME选项
	Freq     int64 // FREQ选项
}

// SortOption SORT命令选项
// TODO
type SortOption struct {
	Pattern string   // 需要排序的key匹配规则
	Offset  int64    //
	Count   int64    //
	Get     []string //
	Sort    string   // 排序规则
	Alpha   bool
	Store   string
}
