package rediss

import "github.com/pyihe/rediss/args"

// Publish v2.0.0后可用
// 命令格式: PUBLISH channel message
// 时间复杂度: O(N+M), N为订阅频道的客户端数量, M是订阅模式的总数
// 发送消息到指定频道。在Redis集群中, 客户端可以发布到每个节点, 集群确保发布的消息根据需要转发, 因此客户端可以通过连接到任何一个节点来订阅任何通道
// 返回值类型: Integer, 返回接收到消息的客户端数量, 在Redis集群中, 只有与发布客户端连接在同一个Redis节点的客户端才会包含在返回值中
func (c *Client) Publish(channel string, message interface{}) (int64, error) {
	cmd := args.Get()
	cmd.Append("PUBLISH", channel)
	cmd.AppendArgs(message)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// PubSubChannels v2.8.0后可用
// 命令格式: PUBSUB CHANNELS [pattern]
// 时间复杂度: O(N), N为活跃的频道数量
// 列出当前活跃的频道(活跃频道是指至少有一个订阅者的发布/订阅频道, 不包含订阅模式的客户端)。
// 如果没有指明pattern, 所有的频道将会被列出, 否则将只会列出与全局模式匹配的频道
// 返回值类型: Array, 返回匹配的每个频道
func (c *Client) PubSubChannels(pattern string) (int64, error) {
	cmd := args.Get()
	cmd.Append("PUBSUB", "CHANNELS")
	if pattern != "" {
		cmd.Append(pattern)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// PubSubHelp v6.2.0后可用
// 命令格式: PUBSUB HELP
// 时间复杂度: O(1)
// 返回秒数子命令的帮助信息
// 返回值类型: Array
func (c *Client) PubSubHelp() (*Reply, error) {
	cmd := args.Get()
	cmd.Append("PUBSUB", "HELP")
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// PubSubNumPat v2.8.0后可用
// 命令格式: PUBSUB NUMPAT
// 时间复杂度: O(1)
// 返回客户端订阅的唯一模式的数量, 不是订阅模式的客户端计数, 而是所有客户端订阅的唯一模式的总数
// 返回值类型: Array
func (c *Client) PubSubNumPat() (*Reply, error) {
	cmd := args.Get()
	cmd.Append("PUBSUB", "NUMPAT")
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	return c.sendCommand(cmdBytes)
}

// PubSubNumSub v2.8.0后可用
// 命令格式: PUBSUB NUMSUB [channel [channel ...]]
// 时间复杂度: O(N), N为请求的channel数量
// 返回指定频道的订阅者数量。如果没有指定任何频道, 将会返回一个空的列表
func (c *Client) PubSubNumSub(channels ...string) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("PUBSUB", "NUMSUB")
	cmd.Append(channels...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	return c.sendCommand(cmdBytes)
}

// PubSubShardChannels v7.0.0开始可用
// 命令格式: PUBSUB SHARDCHANNELS [pattern]
// 时间复杂度: O(N), 其中N是活动分片通道的数量
// 列出当前活动的分片通道, 如果没有指明pattern, 将会列出所有通道
// 返回值类型: Array
func (c *Client) PubSubShardChannels(pattern string) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("PUBSUB", "SHARDCHANNELS")
	if pattern != "" {
		cmd.Append(pattern)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	return c.sendCommand(cmdBytes)
}

// PubSubShardNumSub v7.0.0开始可用
// 命令格式: PUBSUB SHARDNUMSUB [shardchannel [shardchannel ...]]
// 时间复杂度: O(N), N为请求的分片通道数量
// 返回指定分片通道的订阅数量
// 返回值类型: Array
func (c *Client) PubSubShardNumSub(shardChannels ...string) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("PUBSUB", "SHARDNUMSUB")
	cmd.Append(shardChannels...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	return c.sendCommand(cmdBytes)
}

// SPublish v7.0.0开始可用
// 命令格式: SPUBLISH shardchannel message
// 时间复杂度: O(N), N为订阅分片通道的客户端数量
// 向指定分片通道发布消息
// 返回值类型: Integer, 返回接收到消息的客户端数量
func (c *Client) SPublish(shardChannel string, message interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("SPUBLISH", shardChannel)
	cmd.AppendArgs(message)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	return c.sendCommand(cmdBytes)
}
