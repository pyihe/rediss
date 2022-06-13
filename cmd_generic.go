package rediss

import (
	"strconv"
	"sync/atomic"

	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/rediss/args"
	"github.com/pyihe/rediss/model/generic"
)

// Ping v1.0.0后可用
// 命令格式: PING [message]
// 时间复杂度: O(1)
func (c *Client) Ping() (err error) {
	cmd := args.Get()
	cmd.Append("PING")
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	_, err = c.sendCommand(cmdBytes)
	return err
}

// Auth v1.0.0后可用
// 命令格式: AUTH [username] password
// v6.0.0开始支持用户名和密码的风格
// 时间复杂度: O(N), 其中N是为用户定义的密码数量
func (c *Client) Auth(username, password string) error {
	if username == "" {
		username = c.username
	}
	if password == "" {
		password = c.password
	}
	cmd := args.Get()
	cmd.Append("AUTH")
	if username != "" {
		cmd.Append(username)
	}
	cmd.Append(password)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	_, err := c.sendCommand(cmdBytes)
	return err
}

// Select v1.0.0后可用
// 命令格式: SELECT index
// 时间复杂度: O(1)
func (c *Client) Select(database int) error {
	cmd := args.Get()
	cmd.Append("SELECT")
	cmd.AppendArgs(database)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	_, err := c.sendCommand(cmdBytes)
	if err != nil {
		return err
	}
	atomic.StoreInt32(&c.database, int32(database))
	return nil
}

// Copy v6.2.0后可用
// 命令格式: COPY source destination [DB destination-db] [REPLACE]
// 时间复杂度: 对于字符串值为O(1), 最坏为集合的O(N), N为集合中嵌套项的数量
// 将src的值拷贝到dst中, 默认情况下dst在src所在的DB中创建, 但DB选项为dst提供了创建的目标数据库;
// 如果dst已经存在将会返回一个错误, 指定REPLACE选项后, 将会在拷贝前移除dst的值
// 返回值类型: Integer, 如果拷贝成功返回1, 否则返回0
func (c *Client) Copy(src, dst string, dstDB int, replace bool) (bool, error) {
	cmd := args.Get()
	cmd.Append("COPY", src, dst)
	cmd.AppendArgs("DB", dstDB)
	if replace {
		cmd.Append("REPLACE")
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// Migrate v2.6.0后可用
// 命令格式: MIGRATE host port key | "" destination-db timeout [COPY] [REPLACE] [ [AUTH password] | [AUTH2 username password]] [KEYS key [key ...]]
// v3.0.0开始增加COPY和REPLACE选项
// v3.0.6开始增加KEYS选项
// v4.0.7开始增加AUTH选项
// v6.0.0开始增加AUTH2选项
// 时间复杂度: 此命令实际上在源实例中执行DUMP+DEL, 在目标实例中执行 RESTORE; 有关时间复杂度, 请参阅这些命令的页面; 还执行了两个实例之间的O(N)数据传输
// 以原子方式将key从源Redis实例传输到目标Redis实例; 成功后key将从原始实例中删除, 并保证存在于目标实例中
// 该命令是原子的, 并在传输密钥所需的时间内阻塞两个实例, 在任何给定时间key似乎存在于给定实例或另一个实例中, 除非发生超时错误。
// 在3.2及更高版本中, 通过将空字符串 ("") 作为键并添加 KEYS 子句, 可以在对 MIGRATE 的单个调用中对多个key进行流水线化处理
// 该命令在源实例内部使用DUMP生成键值的序列化版本, 并使用 RESTORE 来合成目标实例中的键, 源实例充当目标实例的客户端; 如果目标实例向 RESTORE 命令返回 OK, 则源实例使用 DEL 删除密钥
// 超时指定与目标实例通信的任何时刻的最大空闲时间(以毫秒为单位), 这意味着操作不需要在指定的毫秒数内完成, 但传输不能阻塞超过指定的毫秒数
// MIGRATE 需要执行 I/O 操作并遵守指定的超时; 当传输过程中出现 I/O 错误或达到超时时, 操作将中止并返回特殊错误 - IOERR。发生这种情况时, 可能出现以下两种情:
// 1. key可能同时存在于两个实例
// 2. key可能只在源实例中
//
// 发生超时时密钥不可能丢失. 但调用 MIGRATE 的客户端在发生超时错误时应检查key是否也存在于目标实例中并采取相应措施
// 当返回任何其他错误时(以 ERR 开头), MIGRATE 保证key仍然只存在于原始实例中(除非同名的key也已经存在于目标实例中)
// 如果源实例中没有要迁移的key，则返回 NOKEY; 因为在正常情况下可能会丢失key, 例如从过期开始, NOKEY 就不是错误
//
// 从Redis3.0.6开始, MIGRATE支持一种新的批量迁移模式, 该模式使用管道在实例之间迁移多个键, 而不会产生往返时间延迟和使用单个MIGRATE调用移动每个键时的其他开销
// 为了启用这种形式, 命令使用了KEYS选项, 并将普通键参数设置为空字符串。实际的键名将在 KEYS 参数本身之后提供, 如下例所示:
// MIGRATE 192.168.1.34 6379 "" 0 5000 KEYS key1 key2 key3
// 当使用这种形式时, 只有当实例中不存在任何键时才会返回 NOKEY 状态代码, 否则即使只存在一个键, 也会执行命令
//
// 选项:
// COPY: 不从实例中删除key
// REPLACE: 从远端实例中替代已经存在的key
// KEYS: 如果 key 参数是一个空字符串, 该命令将改为迁移 KEYS 选项后面的所有键
// AUTH: 用给定的密码验证远端实例
// AUTH2: 使用给定的用户名和密码对进行身份验证
//
// 返回值类型: Simple String, 迁移成功返回OK, 如果源实例中没有key, 返回NOKEY
// 函数参数说明:
// host: 目标实例地址
// port: 目标实例端口
// keys: 需要迁移的key, 不管是单个key还是多个key都使用此参数
// dstDB: 需要迁移到目标实例的哪个数据库
// millSec: 超时, 单位毫秒
// copy: 是否添加COPY选项,
// replace: 是否添加REPLACE选项
// username, password: 用户名, 密码, 对于AUTH选项, 只需要提供password, 如果需要使用AUTH2则需要提供username
func (c *Client) Migrate(option *generic.MigrateOption) (string, error) {
	if option == nil {
		return "", ErrEmptyOptionArgument
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("MIGRATE", option.Host, option.Port)
	if len(option.Keys) == 1 {
		cmd.Append(option.Keys[0])
	} else {
		cmd.Append("")
	}
	cmd.AppendArgs(option.Destination, option.Timeout)
	if option.Copy {
		cmd.Append("COPY")
	}
	if option.Replace {
		cmd.Append("REPLACE")
	}
	if option.Password != "" {
		if option.Username == "" {
			cmd.Append("AUTH", option.Password)
		} else {
			cmd.Append("AUTH2", option.Username, option.Password)
		}
	}
	if len(option.Keys) > 1 {
		cmd.Append("KEYS")
		cmd.Append(option.Keys...)
	}

	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return "", err
	}
	return reply.ValueString(), nil
}

// ObjectEncoding v2.2.3后可用
// 命令格式: OBJECT ENCODING key
// 时间复杂度: O(1)
// 返回存储在 <key> 的 Redis 对象的内部编码
// Redis对象可以被以不同的方式编码:
// 1. 字符串可以被编码为raw(普通字符串)或者int(为了节省空间, 表示64位有符号的整型被编码为int)
// 2. 列表可以编码为ziplist或linkedlist; 对于小列表, ziplist是为了节省空间的特殊表示
// 3. 集合可以编码为intset或hashtable; intset 是一种特殊的编码, 用于仅由整数组成的小集合
// 4. 哈希可以编码为ziplist或hashtable; ziplist是一种用于小散列的特殊编码
// 5. 有序集合可以编码为ziplist或skiplist格式, 对于List类型的小有序集合可以使用ziplist进行特殊编码; 而skiplist编码适用于任何大小的排序集
// 返回值类型: Bulk String, 返回编码结果, 如果key不存在返回nil
func (c *Client) ObjectEncoding(key string) (string, error) {
	cmd := args.Get()
	cmd.Append("OBJECT", "ENCODING", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return "", err
	}
	return reply.ValueString(), nil
}

// ObjectFreq v4.0.0后可用
// 命令格式: OBJECT FREQ key
// 时间复杂度: O(1)
// 此命令返回存储在 <key> 的 Redis 对象的对数访问频率计数器
// 该命令仅在 maxmemory-policy 配置指令设置为 LFU 策略之一时可用
// 返回值类型: Integer, 返回计数器值
func (c *Client) ObjectFreq(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("OBJECT", "FREQ", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// ObjectHelp v6.2.0后可用
// 命令格式: OBJECT HELP
// 时间复杂度: O(1)
// OBJECT HELP 命令返回描述不同子命令的有用文本
// 返回值类型: Array, 返回子命令和它们的描述的数组
func (c *Client) ObjectHelp() (*Reply, error) {
	cmd := args.Get()
	cmd.Append("OBJECT", "HELP")
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// ObjectIdleTime v2.2.3后可用
// 命令格式: OBJECT IDLETIME key
// 时间复杂度: O(1)
// 此命令返回自上次访问存储在 <key> 中的值以来的时间(以秒为单位)
// 该命令仅在 maxmemory-policy 配置指令未设置为 LFU 策略之一时可用
// 返回值类型: Integer, 以秒为单位返回时间
func (c *Client) ObjectIdleTime(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("OBJECT", "IDLETIME", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// ObjectRefCount v2.2.3后可用
// 命令格式: OBJECT REFCOUNT key
// 时间复杂度: O(1)
// 此命令返回存储在 <key> 处值的引用计数
// 返回值类型: Integer, 返回值被引用的数
func (c *Client) ObjectRefCount(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("OBJECT", "REFCOUNT", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Restore v2.6.0后可用
// 命令格式: RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
// v3.0.0开始支持REPLACE修饰符
// v5.0.0开始支持ABSTTL修饰符
// v5.0.0开始支持IDLETIME和FREQ修饰符
// 时间复杂度: O(1)创建新键, 另外O(NM)重构序列化值, 其中N是组成该值的Redis对象的数量, M是它们的平均大小; 对于小字符串值, 时间复杂度因此为O(1)+O(1M), 其中M很小, 因此只需O(1), 然而对于有序集合, 复杂度是O(NMlog(N)), 因为将值插入排序集是O(log(N))
// 通过反序列化value创建key, 其中value为通过DUNP命令获取的值
// ttl为key的有效期, 单位为毫秒, ttl为0表示key没有过期时间, 如果使用了ABSTTL, 则ttl表示key过期的毫秒时间戳
// 处于驱逐目的, 可以使用IDLETIME或者FREQ修饰符
// 如果key早已经存在, RESTORE将会返回"Target key name is busy"的错误, 除非使用了REPLACE修饰符
// RESTORE将会校验RDB版本和数据校验和, 如果不匹配将会返回错误
// 返回值类型: Simple String, 成功返回OK
func (c *Client) Restore(key string, value string, option *generic.RestoreOption) (string, error) {
	if option == nil {
		return "", ErrEmptyOptionArgument
	}
	cmd := args.Get()
	cmd.Append("RESTORE", key)
	cmd.AppendArgs(option.TTL, value)
	if option.Replace {
		cmd.Append("REPLACE")
	}
	if option.ABSTTL {
		cmd.Append("ABSTTL")
	}
	if option.IdleTime > 0 {
		cmd.AppendArgs("IDLETIME", option.IdleTime)
	}
	if option.Freq > 0 {
		cmd.AppendArgs("FREQ", option.Freq)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return "", err
	}
	return reply.ValueString(), nil
}

// Sort v1.0.0后可用
// 命令格式: SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ ASC | DESC] [ALPHA] [STORE destination]
// 时间复杂度: O(N+M*log(M)), 其中N是列表中或要排序的元素的数量, M是返回元素的数量; 当元素未排序时, 复杂度为 O(N)
// 返回或存储列表、集合或有序集合中包含的元素
// 返回值类型:
// 1. 没有传递store参数, 返回排序后元素的列表
// 2. 如果指明了store参数, 返回存储在dst列表中的元素数量
func (c *Client) Sort(key string, option *generic.SortOption) (*Reply, error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	cmd := args.Get()
	cmd.Append("SORT", key)
	if option.Pattern != "" {
		cmd.Append("BY", option.Pattern)
	}
	if option.Count > 0 && option.Offset >= 0 {
		cmd.AppendArgs("LIMIT", option.Offset, option.Count)
	}
	for _, get := range option.Get {
		cmd.Append("GET", get)
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	if option.Alpha {
		cmd.Append("ALPHA")
	}
	if option.Store != "" {
		cmd.Append("STORE", option.Store)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SortRo v7.0.0后可用
// Sort的只读版本
func (c *Client) SortRo(key string, option *generic.SortOption) (*Reply, error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	if option.Store != "" {
		return nil, errors.New("SORT_RO not support STORE argument")
	}
	cmd := args.Get()
	cmd.Append("SORT_RO", key)
	if option.Pattern != "" {
		cmd.Append("BY", option.Pattern)
	}
	if option.Count > 0 && option.Offset >= 0 {
		cmd.AppendArgs("LIMIT", option.Offset, option.Count)
	}
	for _, get := range option.Get {
		cmd.Append("GET", get)
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	if option.Alpha {
		cmd.Append("ALPHA")
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	return c.sendCommand(cmdBytes)
}

// Touch v3.2.1后可用
// 命令格式: TOUCH key [key...]
// 时间复杂度: O(N), N为keys的数量
// 更改key的最后访问时间; 如果键不存在, 则忽略它
// 返回值类型: 被修改的key的数量
func (c *Client) Touch(keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("TOUCH")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Unlink v4.0.0后可用
// 命令格式: UNLINK key [key ...]
// 时间复杂度: O(1)对于每个删除的键, 无论其大小如何; 然后该命令在不同的线程中执行O(N)工作以回收内存, 其中N是已删除对象的分配数
// 此命令与DEL非常相似: 它删除指定的key; 就像DEL一样, 如果一个key不存在, 它就会被忽略。
// 但是该命令在不同的线程中执行实际的内存回收, 因此它不是阻塞的, 而DEL是。这就是命令名称的来源: 该命令只是将key与key空间取消链接。实际的删除将在稍后异步发生
// 返回值类型: Integer, 被删除的key的数量
func (c *Client) Unlink(keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("UNLINK")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Wait v3.0.0后可用
// 命令格式: WAIT numreplicas timeout
// 时间复杂度: O(1)
// 此命令阻塞当前客户端, 直到所有先前的写入命令都成功传输并至少被指定数量的副本确认; 如果达到超时(以毫秒为单位), 即使尚未达到指定数量的副本, 命令也会返回
// 命令将始终返回确认在 WAIT 命令之前发送的写入命令的副本数, 无论是在达到指定副本数的情况下, 还是在达到超时的情况下
// 绩点备注:
// 1. WAIT返回时, 以WAIT返回的副本数保证之前在当前连接的上下文中发送的所有写命令都被接收
// 2. 如果命令作为 MULTI 事务的一部分发送, 则该命令不会阻塞, 而是尽快返回确认先前写入命令的副本数量
// 3. timeout为0时意味着永久阻塞
// 4. 由于 WAIT 返回在失败和成功的情况下达到的副本数, 客户端应检查返回的值是否等于或大于它要求的复制级别
// 返回值类型: Integer, 该命令返回在当前连接的上下文中执行的所有写入所达到的副本数
func (c *Client) Wait(numRep int64, timeout int64) (int64, error) {
	cmd := args.Get()
	cmd.Append("WAIT")
	cmd.AppendArgs(numRep, timeout)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Del v1.0.0后可用
// 命令格式: DEL key [key ...]
// 时间复杂度: O(N), N为被移除的key的数量; 当key存储的数据类型不是string时, 复杂度为O(M), M为key所存储数据类型的元素个数
// 移除一个存储string类型的key的复杂度为O(1)
// 移除指定的key, 当key不存在时, 将会被忽略
// 返回值类型: Integer, 返回被移除的key的数量
func (c *Client) Del(keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.AppendArgs("DEL")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Dump v2.6.0后可用
// 命令格式: DUMP key
// 时间复杂度: O(1)为找到key的复杂度, 序列化的复杂度为O(NM), N为组成值的redis对象的数量, M为对象的平均大小
// 以Redis特定的格式序列化存储在key中的值并将其返回给用户, 可以使用RESTORE命令将返回的值合成回Redis键
// 序列化格式是不透明且非标准的, 但它具有一些语义特征:
// 1. 它包含一个64位校验和, 用于确保检测到错误; RESTORE命令确保在使用序列化值合成密钥之前检查校验和
// 2. 值被序列化的格式与RDB使用的相同
// 3. 一个RDB版本被编码在序列化值内部, 因此RDB格式不兼容的不同Redis版本将拒绝处理序列化值
// 序列化值不包含过期信息; 为了捕获当前值的生存时间, 应使用PTTL命令
// 如果key不存在, 返回nil。否则返回序列化之后的值
// 返回值类型: Bulk String, 序列化值
func (c *Client) Dump(key string) (string, error) {
	cmd := args.Get()
	cmd.AppendArgs("DUMP", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return "", err
	}
	return reply.ValueString(), err
}

// Exists v1.0.0后可用
// 命令格式: EXISTS key [key ...]
// 时间复杂度: O(N), N为key的数量
// 判断key是否存在
// 用户应该知道, 如果在参数中多次提到相同的现有key, 它将被计算多次; 所以如果somekey存在, EXISTS somekey somekey将返回2
// 返回值类型: Integer, 返回提供的key中存在的数量
func (c *Client) Exists(keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.AppendArgs("EXISTS")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Expire v1.0.0后可用
// 命令格式: EXPIRE key seconds [ NX | XX | GT | LT]
// v7.0.0开始支持NX, XX, GT, LT选项
// 时间复杂度: O(1)
// 设置key的有效期时间, 单位为秒
// 超时时间只会被删除或者覆盖的命令清除, 比如DEL, SET, GETSET等
// 所有从概念上改变存储在键中的值而不用新值替换它的操作将使超时保持不变, 比如LPUSH, INCR等
// 使用PERSIST命令同样将会使timeout被清除
// key被重命名时, timeout将会转移到新的key中
// EXPIRE/PEXPIRE设置负数的timeout以及EXPIREAT/PEXPIREAT将会使key被删除而不是过期
// 可选项:
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 对于已经设置了timeout的key, EXPIRE将会返回0并且不会更改既有的timeout
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) Expire(key string, sec int64, op string) (bool, error) {
	cmd := args.Get()
	cmd.Append("EXPIRE", key)
	cmd.AppendArgs(sec)
	if op != "" {
		cmd.Append(op)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// ExpireAt v1.2.0后可用
// 命令格式: EXPIREAT key unix-time-seconds [ NX | XX | GT | LT]
// v7.0.0开始支持NX, XX, GT, LT选项
// 时间复杂度: O(1)
// EXPIREAT与EXPIRE一样, 不同的是EXPIREAT设置的是时间戳
// 选项:
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) ExpireAt(key string, unix int64, op string) (bool, error) {
	cmd := args.Get()
	cmd.Append("PEXPIREAT", key)
	cmd.AppendArgs(unix)
	if op != "" {
		cmd.Append(op)
	}
	cmdBts := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBts)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// ExpireTime v7.0.0后可用
// 命令格式: EXPIRETIME key
// 时间复杂度: O(1)
// 返回给定key的过期时间戳, 格式为时间戳, 精确到秒
// 返回值类型: Integer, 返回key过期的时间戳, 负数标识错误:
// 1. key存在但没有设置过期时间时返回-1
// 2. key不存在返回-2
func (c *Client) ExpireTime(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("EXPIRETIME", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// PExpire v2.6.0后可用
// 命令格式: PEXPIRE key milliseconds [ NX | XX | GT | LT]
// v7.0.0开始支持NX, XX, GT, LT选项
// 时间复杂度: O(1)
// 设置key的过期时间, 单位为毫秒
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) PExpire(key string, millSec int64, op string) (bool, error) {
	cmd := args.Get()
	cmd.AppendArgs("PEXPIRE", key)
	cmd.AppendArgs(millSec)
	if op != "" {
		cmd.Append(op)
	}
	cmdBts := cmd.Bytes()
	args.Put(cmd)
	reply, err := c.sendCommand(cmdBts)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// PExpireAt v2.6.0后可用
// 命令格式: PEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT]
// v7.0.0开始支持NX, XX, GT, LT选项
// 时间复杂度: O(1)
// 设置key的过期时间点, 时间点单位为毫秒
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) PExpireAt(key string, millUnix int64, op string) (bool, error) {
	cmd := args.Get()
	cmd.AppendArgs("PEXPIREAT", key)
	cmd.AppendArgs(millUnix)
	if op != "" {
		cmd.Append(op)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// PExpireTime v7.0.0后可用
// 命令格式: PEXPIRETIME key
// 时间复杂度: O(1)
// 获取key过期时间戳, 精确到毫秒
// 返回值类型: 返回key过期的时间戳, 负数标识错误:
// 1. key存在但没有设置过期时间时返回-1
// 2. key不存在返回-2
func (c *Client) PExpireTime(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("PEXPIRETIME", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// Keys v1.0.0后可用
// 命令格式: KEYS pattern
// 时间复杂度: O(N), N为数据库中的key数量
// 返回所有符合给定模式pattern的key
// 返回值类型: Array, 匹配到的key的数组
func (c *Client) Keys(pattern string) (result []string, err error) {
	cmd := args.Get()
	cmd.AppendArgs("KEYS", pattern)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}
	return reply.parseKeysResult()
}

// Move v1.0.0后可用
// 命令格式: MOVE key db
// 时间复杂度: O(1)
//用于将当前数据库指定的key移动到给定的数据库中
// 返回值类型: Integer, 成功返回1, 失败返回0
func (c *Client) Move(key string, targetDB int) (bool, error) {
	cmd := args.Get()
	cmd.AppendArgs("MOVE", key, targetDB)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// Persist v2.2.0后可用
// 命令格式: PERSIST key
// 时间复杂度: O(1)
// 移除key的过期时间, key将永久保持
// 返回值类型: Integer, 移除成功返回1, key不存在或者key没有过期时间, 返回0
func (c *Client) Persist(key string) (bool, error) {
	cmd := args.Get()
	cmd.AppendArgs("PERSIST", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// PTTL v2.6.0后可用
// 命令格式: PTTL key
// 时间复杂度: O(1)
// 以毫秒为单位返回key剩余的过期时间
// 在2.6版本以前, 如果key不存在或者key存在但没有过期时间时都返回-1
// 2.6版本后, 如果key不存在返回-1, 如果key存在但没有过期时间时返回-2
// 返回值类型: Integer, key不存在时返回-2, key存在但没有过期时间时返回-1, 否则以毫秒为单位返回剩余过期时间
func (c *Client) PTTL(key string) (int64, error) {
	cmd := args.Get()
	cmd.AppendArgs("PTTL", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// TTL v1.0.0后可用
// 命令格式: TTL key
// 时间复杂度: O(1)
// 以秒为单位返回key剩余的过期时间
// 返回值类型: Integer, key不存在时返回-2, key存在但没有过期时间时返回-1, 否则以秒为单位返回剩余过期时间
func (c *Client) TTL(key string) (int64, error) {
	cmd := args.Get()
	cmd.AppendArgs("TTL", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

//
// RandomKey v1.0.0后可用
// 命令格式: RANDOMKEY
// 时间复杂度: O(1)
// 从当前数据库中随机返回一个key
// 返回值类型: Bulk String, 如果数据库没有key, 返回nil, 否则随机返回一个key
func (c *Client) RandomKey() (string, error) {
	cmd := args.Get()
	cmd.AppendArgs("RANDOMKEY")
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return "", err
	}
	return reply.ValueString(), nil
}

// Rename v1.0.0后可用
// 命令格式: RENAME key newkey
// 时间复杂度: O(1)
// 修改key的名称为newKey
// 当newKey已经存在时, 其值将会被覆盖
// 返回值类型: Simple String, 修改成功时返回OK, 失败返回错误
// v3.2.0后, 如果key和newKey相同不再返回错误
func (c *Client) Rename(key, newKey string) (bool, error) {
	cmd := args.Get()
	cmd.AppendArgs("RENAME", key, newKey)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// RenameNX v1.0.0后可用
// 命令格式: RENAMENX key newkey
// 时间复杂度: O(1)
// 仅当newKey不存在时将key改名为newKey
// 返回值类型: 修改成功返回1, 如果newKey已经存在返回0
// v3.2.0后, 如果key和newKey相同不再返回错误
func (c *Client) RenameNX(key, newKey string) (bool, error) {
	cmd := args.Get()
	cmd.AppendArgs("RENAMENX", key, newKey)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// Scan v2.8.0后可用
// 命令格式: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
// v6.0.0后添加TYPE参数
// 时间复杂度: O(N), N为scan的元素数量
// 用于迭代数据库中的key
// SCAN 命令是一个基于游标的迭代器, 每次被调用之后, 都会向用户返回一个新的游标, 用户在下次迭代时需要使用这个新游标作为SCAN命令的游标参数, 以此来延续之前的迭代过程。
// SCAN返回一个包含两个元素的数组, 第一个元素是用于进行下一次迭代的新游标, 而第二个元素则是一个数组, 这个数组中包含了所有被迭代的元素。
// 如果新游标返回0表示迭代已结束。
// 命令格式: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
// COUNT: 每次最多迭代多少个元素
// MATCH: 只迭代给定样式的元素
// TYPE: 遍历的值类型, 如ZSET, GEOHASH
// 返回值类型: Array, 返回遍历的key的数组
func (c *Client) Scan(cursor int, option *generic.ScanOption) (result *generic.ScanResult, err error) {
	cmd := args.Get()
	cmd.Append("SCAN", strconv.FormatInt(int64(cursor), 10))
	if option != nil {
		if option.Match != "" {
			cmd.Append("MATCH", option.Match)
		}
		if option.Count > 0 {
			cmd.AppendArgs("COUNT", option.Count)
		}
		if option.Type != "" {
			cmd.Append("TYPE", option.Type)
		}
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}

	return reply.parseScanResult()
}

// Type v1.0.0后可用
// 命令格式: TYPE key
// 返回key所存储的值的类型
// 返回值类型:
// none: key不存在
// string: 字符串
// list: 列表
// set: 集合
// zset: 有序集
// hash: 哈希表
func (c *Client) Type(key string) (string, error) {
	cmd := args.Get()
	cmd.AppendArgs("TYPE", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return "", err
	}
	return reply.ValueString(), nil
}
