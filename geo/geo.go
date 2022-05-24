package geo

// Location 地理空间信息
type Location struct {
	Longitude float64 // 经度
	Latitude  float64 // 纬度
	Name      string  // 地理名
	Distance  float64 // 离中心位置的距离
	GeoHash   int64   // geohash编码
}

// RadiusOption GEORADIUS命令的查询选项
type RadiusOption struct {
	Radius    float64 // 半径
	Unit      string  // 单位: M(米), KM(公里), FT(英尺), MI(英里)
	WithCoord bool    // 同时返回经纬度坐标
	WithDist  bool    // 同时返回离圆点的距离
	WithHash  bool    // 同时返回GEOHASH编码
	Any       bool    // 是否指定COUNT中的any参数
	Count     int64   // count参数
	Sort      string  // 排序方式
	Store     string  //
	StoreDist string  //
}

// SearchOption GEOSEARCH选项
type SearchOption struct {
	// 以成员为中心
	Member string // 以member为中心时的member名称

	// 以指定的经纬度为中心
	Longitude float64 // 经度
	Latitude  float64 // 纬度

	// 扫描圆形区域的半径
	Radius     float64 // 查询半径
	RadiusUnit string  // 圆形范围单位: M(米), KM(公里), FT(英尺), MI(英里)

	// 扫描矩形区域
	Width   float64 // 矩形宽度
	Height  float64 // 矩形高度
	BoxUnit string  // 矩形范围的单位: M(米), KM(公里), FT(英尺), MI(英里)

	Sort      string // 排序方式
	Count     int64  // COUNT参数
	Any       bool   //
	WithCoord bool   // 同时返回经纬度坐标
	WithDist  bool   // 同时返回离圆点的距离
	WithHash  bool   // 同时返回GEOHASH编码
	StoreDist string //
}
