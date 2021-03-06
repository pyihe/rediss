package rediss

import (
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/rediss/args"
	"github.com/pyihe/rediss/model/geo"
)

// GeoAdd v3.2.0后可用
// 命令格式: GEOADD key [ NX | XX] [CH] longitude latitude member [ longitude latitude member ...]
// v6.2.0开始支持CH, NX, XX选项
// 时间复杂度: 对于每个添加项的复杂度为O(log(N)), 其中N为有序集合中的元素数量
// 添加指定的地理位置(经度, 纬度, 名称)项到指定的key中, 数据存储在有序集合中
// 命令参数采用标准的x, y格式, 所以必须在纬度之前指定经度
// 可以索引的坐标有限制: 非常靠近两极的区域不可索引
// 1. 有效经度为 -180 到 180 度
// 2. 有效纬度为 -85.05112878 到 85.05112878 度
// 如果想要索引指定范围外的坐标, 命令将会报错
// 命令选项:
// XX: 不添加新元素, 只更新已经存在的元素
// NX: 只添加新元素, 不更新已经存在的元素
// CH: 将返回值类型从添加新元素的数量修改为改变的元素总数
// 返回值类型: Integer
// 如果没有指定可选参数, 返回添加的新元素数量
// 如果指定了CH选项, 返回添加或者更新的元素数量
func (c *Client) GeoAdd(key, op string, members ...*geo.Location) (int64, error) {
	if len(members) == 0 {
		return 0, errors.New("GEOADD need pass member arguments at least one")
	}

	cmd := args.Get()
	cmd.Append("GEOADD", key)
	if op != "" {
		cmd.Append(op)
	}
	for _, me := range members {
		cmd.AppendArgs(me.Longitude, me.Latitude, me.Name)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// GeoDist v3.2.0后可用
// 命令格式: GEODIST key member1 member2 [ M | KM | FT | MI]
// 时间复杂度: O(log(N))
// 返回两个成员的距离
// 如果member1和member2都缺失, 命令将会返回nil
// 单位描述:
// M: 单位米
// KM: 单位公里
// MI: 单位英里
// FT: 单位英尺
// 返回值类型: Bulk String, 返回nil或者双精度浮点数的距离
func (c *Client) GeoDist(key string, member1, member2 string, unit string) (float64, error) {
	cmd := args.Get()
	cmd.Append("GEODIST", key, member1, member2)
	if unit != "" {
		cmd.Append(unit)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Float()
}

// GeoHash v3.2.0后可用
// 命令格式: GEOHASH key member [member ...]
// 时间复杂度: 对于每个请求的成员复杂度为: O(log(N)), 其中N为有序集合中的元素数量
// 返回一个或者多个地理位置的有效GeoHash字符串
// 返回值类型: Array
func (c *Client) GeoHash(key string, members ...string) (result []string, err error) {
	cmd := args.Get()
	cmd.Append("GEOHASH", key)
	cmd.Append(members...)

	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}

	return reply.parseGeoHashResult()
}

// GeoPos v3.2.0后可用
// 命令格式: GEOPOS key member [member ...]
// 时间复杂度: O(N), N为请求的member数量
// 返回指定成员的经纬度
// 返回值类型: Array, 不存在的元素返回值将会是nil
func (c *Client) GeoPos(key string, members ...string) (result []*geo.Location, err error) {
	// 组装参数
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEOPOS", key)
	cmd.Append(members...)

	// 获取回复
	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}
	return reply.parseGeoPosResult(members...)
}

// GeoRadiusRo v3.2.10后可用, v6.2.0后被视为废弃
// 命令格式: GEORADIUS_RO key longitude latitude radius M | KM | FT | MI [WITHCOORD] [WITHDIST] [WITHHASH] [ COUNT count [ANY]] [ ASC | DESC]
// v6.2.0开始支持ANY选项
// 时间复杂度: O(N+log(M))其中N是由中心和半径分隔的圆形区域的边界框内的元素数, M是索引内的项目数
// GEORADIUS的只读变种
func (c *Client) GeoRadiusRo(key string, longitude, latitude float64, option *geo.RadiusOption) (result []*geo.Location, err error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}

	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEORADIUS_RO", key)
	cmd.AppendArgs(longitude, latitude)
	cmd.AppendArgs(option.Radius)

	if option.StoreDist != "" || option.Store != "" {
		return nil, errors.New("GEORADIUS_RO not support STORE or STOREDIST arguments")
	}

	if option.Unit != "" {
		cmd.Append(option.Unit)
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}

	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}

	return reply.parseGeoLocation(option)
}

// GeoRadius v3.2.0后可用, v6.2.0开始被视为废弃
// 命令格式: GEORADIUS key longitude latitude radius M | KM | FT | MI [WITHCOORD] [WITHDIST] [WITHHASH] [ COUNT count [ANY]] [ ASC | DESC] [STORE key] [STOREDIST key]
// v6.2.0开始支持ANY选项
// 时间复杂度: O(N+log(M))其中N是由中心和半径分隔的圆形区域的边界框内的元素数, M是索引内的项目数
// 返回指定经纬度指定半径内的成员
// 单位说明: M(单位米), KM(单位公里), MI(单位英里), FT(单位英尺)
// 可选项说明:
// WITHDIST: 同时返回每个返回项与中心点的距离, 距离单位都一样, 由命令参数指定
// WITHCOORD: 返回每个匹配项的经纬度坐标
// WITHHASH: 返回没有匹配项的geohash编码(有序集合的分数), 格式为52位无符号整数
//
// 命令默认返回无序的匹配项, 下面的选项可以进行排序
// ASC: 按照离中心位置的距离由近到远排序
// DESC: 按照离中心位置的距离由远到近排序
//
// 默认情况所有的匹配项都会被返回, 但可以通过COUNT参数指定返回匹配集合中的前count个;
// 当提供ANY时, 一旦找到足够的匹配, 该命令将立即返回, 因此结果可能不是最接近指定点的结果
// 但另一方面, 服务器投入的工作量要低得多; 当没有提供ANY时, 该命令将执行与匹配指定区域的项目数成正比的工作并对其进行排序
// 因此使用非常小的COUNT选项查询非常大的区域可能会很慢, 即使只有几个结果是回来
//
// 默认情况下, 该命令将项目返回给客户端。可以使用以下选项之一存储结果:
// STORE: 利用返回项的地理位置信息将它们存储在有序集合中
// STOREDIST: 将返回项离中心位置的距离作为分数(单位为命令中指定的单位)存储在有序集合中
//
// 返回值类型: Array
// 没有指定任何WITH选项, 命令只返回一个线性数组
// 如果指定了WITHCOORD, WITHDIST或者WITHHASH选项, 命令返回由数组组成的数组, 每个子数组元素代表一个返回项
func (c *Client) GeoRadius(key string, longitude, latitude float64, option *geo.RadiusOption) (result []*geo.Location, err error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	if option.Store != "" || option.StoreDist != "" {
		return nil, errors.New("GeoRadius not support STORE or STOREDIST arguments")
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEORADIUS", key)
	cmd.AppendArgs(longitude, latitude)
	cmd.AppendArgs(option.Radius)
	if option.Unit != "" {
		cmd.Append(option.Unit)
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	if option.Store != "" {
		cmd.Append("STORE", option.Store)
	}
	if option.StoreDist != "" {
		cmd.Append("STOREDIST", option.StoreDist)
	}

	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}
	return reply.parseGeoLocation(option)
}

func (c *Client) GeoRadiusStore(key string, longitude, latitude float64, option *geo.RadiusOption) (*Reply, error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	if option.Store == "" && option.StoreDist == "" {
		return nil, errors.New("GeoRadiusStore need STORE or STOREDIST arguments")
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEORADIUS", key)
	cmd.AppendArgs(longitude, latitude)
	cmd.AppendArgs(option.Radius)
	if option.Unit != "" {
		cmd.Append(option.Unit)
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	if option.Store != "" {
		cmd.Append("STORE", option.Store)
	}
	if option.StoreDist != "" {
		cmd.Append("STOREDIST", option.StoreDist)
	}

	return c.sendCommand(cmd.Bytes())
}

// GeoRadiusByMember v3.2.0后可用, v6.2.0开始被视为废弃
// 命令格式: GEORADIUSBYMEMBER key member radius M | KM | FT | MI [WITHCOORD] [WITHDIST] [WITHHASH] [ COUNT count [ANY]] [ ASC | DESC] [STORE key] [STOREDIST key]
// 时间复杂度: O(N+log(M))其中N是由中心和半径分隔的圆形区域的边界框内的元素数, M是索引内的项目数
// 此命令与 GEORADIUS 完全一样, 唯一的区别是, 它不是将经度和纬度值作为要查询的区域的中心, 而是采用排序集表示的地理空间索引中已经存在的成员的名称
func (c *Client) GeoRadiusByMember(key, member string, option *geo.RadiusOption) (result []*geo.Location, err error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	if option.Store != "" || option.StoreDist != "" {
		return nil, errors.New("GeoRadiusByMember not support STORE or STOREDIST arguments")
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEORADIUSBYMEMBER", key, member)
	cmd.AppendArgs(option.Radius)
	if option.Unit != "" {
		cmd.Append(option.Unit)
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}
	return reply.parseGeoLocation(option)
}

func (c *Client) GeoRadiusByMemberStore(key, member string, option *geo.RadiusOption) (*Reply, error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	if option.Store == "" && option.StoreDist == "" {
		return nil, errors.New("GeoRadiusByMemberStore need STORE or STOREDIST arguments")
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEORADIUSBYMEMBER", key, member)
	cmd.AppendArgs(option.Radius)
	if option.Unit != "" {
		cmd.Append(option.Unit)
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	return c.sendCommand(cmd.Bytes())
}

// GeoRadiusByMemberRo v3.2.10和4.0.0后可用
// 命令格式: GEORADIUSBYMEMBER key member radius M | KM | FT | MI [WITHCOORD] [WITHDIST] [WITHHASH] [ COUNT count [ANY]] [ ASC | DESC]
// 时间复杂度: O(N+log(M))其中N是由中心和半径分隔的圆形区域的边界框内的元素数, M是索引内的项目数
// 此命令与为GEORADIUSBYMEMBER的只读版本
func (c *Client) GeoRadiusByMemberRo(key, member string, option *geo.RadiusOption) (result []*geo.Location, err error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEORADIUSBYMEMBER_RO", key, member)
	cmd.AppendArgs(option.Radius)
	if option.StoreDist != "" || option.Store != "" {
		return nil, errors.New("GeoRadiusByMemberRo not support STORE or STOREDIST arguments")
	}
	if option.Unit != "" {
		cmd.Append(option.Unit)
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}
	return reply.parseGeoLocation(option)
}

// GeoSearch v6.2.0后可用
// 命令格式: GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude BYRADIUS radius M | KM | FT | MI | BYBOX width height M | KM | FT | MI [ ASC | DESC] [ COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
// 时间复杂度: O(N+log(M))其中N是作为过滤器提供的形状周围的网格对齐边界框区域中的元素数, M是形状内的项目数
// 返回指定形状区域内的地理位置
// 参数说明:
// FROMMEMBER: 以有序集合的成员作为中心
// FROMLONLAT: 使用给定的经纬度作为中心
// BYRADIUS: 根据给定的半径扫描圆形区域
// BYBOX: 根据给定的宽度和高度扫描轴对齐的矩形
// ASC: 按照离中心位置的距离由近到远排序
// DESC: 按照离中心位置的距离由远到近排序
// WITHDIST: 同时返回每个返回项与中心点的距离, 距离单位都一样, 由命令参数指定
// WITHCOORD: 返回每个匹配项的经纬度坐标
// WITHHASH: 返回没有匹配项的geohash编码(有序集合的分数), 格式为52位无符号整数
// 返回值类型: Array,
// 没有指定任何WITH选项, 命令只返回一个线性数组
// 如果指定了WITHCOORD, WITHDIST或者WITHHASH选项, 命令返回由数组组成的数组, 每个子数组元素代表一个返回项
func (c *Client) GeoSearch(key string, option *geo.SearchOption) (result []*geo.Location, err error) {
	if option == nil {
		return nil, ErrEmptyOptionArgument
	}
	if option.StoreDist != "" {
		return nil, errors.New("GeoSearch not support STOREDIST argument")
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEOSEARCH", key)
	if option.Member != "" {
		cmd.Append("FROMMEMBER", option.Member)
	} else {
		cmd.Append("FROMLONLAT")
		cmd.AppendArgs(option.Longitude, option.Latitude)
	}
	if option.Radius > 0 {
		cmd.Append("BYRADIUS")
		cmd.AppendArgs(option.Radius)
		if option.RadiusUnit != "" {
			cmd.Append(option.RadiusUnit)
		}
	} else {
		cmd.Append("BYBOX")
		cmd.AppendArgs(option.Width, option.Height)
		if option.BoxUnit != "" {
			cmd.Append(option.BoxUnit)
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.StoreDist != "" {
		cmd.Append(option.StoreDist)
	}
	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return nil, err
	}
	return reply.parseGeoLocation(option)
}

// GeoSearchStore v6.2.0后可用
// 命令格式: GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT longitude latitude BYRADIUS radius M | KM | FT | MI | BYBOX width height M | KM | FT | MI [ ASC | DESC] [ COUNT count [ANY]] [STOREDIST]
// 时间复杂度: O(N+log(M))其中N是作为过滤器提供的形状周围的网格对齐边界框区域中的元素数, M是形状内的项目数
// 返回指定形状区域内的地理位置
// 返回值类型: Array,
// 没有指定任何WITH选项, 命令只返回一个线性数组
// 如果指定了WITHCOORD, WITHDIST或者WITHHASH选项, 命令返回由数组组成的数组, 每个子数组元素代表一个返回项
func (c *Client) GeoSearchStore(key string, option *geo.SearchOption) (int64, error) {
	if option == nil {
		return 0, ErrEmptyOptionArgument
	}
	if option.StoreDist == "" {
		return 0, errors.New("GeoSearchStore need STOREDIST argument")
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("GEOSEARCHSTORE", key)
	if option.Member != "" {
		cmd.Append("FROMMEMBER", option.Member)
	} else {
		cmd.Append("FROMLONLAT")
		cmd.AppendArgs(option.Longitude, option.Latitude)
	}
	if option.Radius > 0 {
		cmd.Append("BYRADIUS")
		cmd.AppendArgs(option.Radius)
		if option.RadiusUnit != "" {
			cmd.Append(option.RadiusUnit)
		}
	} else {
		cmd.Append("BYBOX")
		cmd.AppendArgs(option.Width, option.Height)
		if option.BoxUnit != "" {
			cmd.Append(option.BoxUnit)
		}
	}
	if option.Sort != "" {
		cmd.Append(option.Sort)
	}
	if option.Count > 0 {
		cmd.Append("COUNT")
		cmd.AppendArgs(option.Count)
		if option.Any {
			cmd.Append("ANY")
		}
	}
	if option.WithCoord {
		cmd.Append("WITHCOORD")
	}
	if option.WithDist {
		cmd.Append("WITHDIST")
	}
	if option.WithHash {
		cmd.Append("WITHHASH")
	}
	if option.StoreDist != "" {
		cmd.Append(option.StoreDist)
	}

	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}
