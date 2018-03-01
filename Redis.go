// 动态、易变、低风险数据存放于redis
// 固定key，使用INCR生成自增的MSGID
// 请求方自定义key，在一段时间内保存消息的特殊标记
// 固定key，使用LIST作为广播消息队列
// 固定key，使用HASH保存消息的Ack信息
// 固定key，使用ZSET保存群组消息ID和它的生命周期，用户收到群组消息后，使用SET做标记
// 用户ID为key，使用ZSET保存用户消息ID和它的生存周期，用户收到消息后，从ZSET中删除
// DeviceKey为key，使用ZSET保存设备消息ID和它的生存周期，设备收到消息后，从ZSET中删除
// 当使用ZSET时以精确到毫秒的int64时间为scroe，如20060102150405999, 在特殊情况下score=0
package MsgStore

import (
	"fmt"
	"github.com/6xiao/go/Common"
	"github.com/garyburd/redigo/redis"
	"time"
)

type Redis struct {
	pool *redis.Pool
}

// 使用redis连接池，用前Get，用完Close
func NewMutableStore(addr string, nrDb int) *Redis {
	defer Common.CheckPanic()

	pool := &redis.Pool{
		MaxIdle:     8,
		IdleTimeout: time.Minute,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}

			_, err = c.Do("SELECT", nrDb)
			return c, err
		},
	}

	return &Redis{pool}
}

// redis的incr操作是原子性递增的数字，可以用来生成msgid
func (this *Redis) NewMsgID(key string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Int64(rc.Do("INCR", key))
}

// 标识请求的哈希值和消息ID的映射关系
func (this *Redis) MarkRequest(key, value string, ttl int) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	rc.Do("SETEX", key, ttl, value)
}

// 根据哈希值查询消息ID
func (this *Redis) GetRequest(key string) (string, error) {
	if len(key) == 0 {
		return "", nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	res, err := rc.Do("GET", key)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}

	return redis.String(res, nil)
}

// 广播队列，使用列表LIST
// 新消息广播队列, 返回队列长度
func (this *Redis) PublishNewMsg(queue string, msg []byte) (int64, error) {
	if len(queue) == 0 || len(msg) == 0 {
		return 0, nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Int64(rc.Do("RPUSH", queue, msg))
}

// 从消息广播队列取一个消息，非阻塞，需要循环调用
func (this *Redis) GetPublishMsg(queue string) ([]byte, error) {
	if len(queue) == 0 {
		return nil, nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	// 返回ErrNil是因为队列空，算不上错误
	msg, err := redis.Bytes(rc.Do("LPOP", queue))
	if err == redis.ErrNil {
		return nil, nil
	}

	return msg, err
}

// 延时推送缓存，使用zset
func (this *Redis) NewLazyMsg(set string, tm time.Time, msgid string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Int64(rc.Do("ZADD", set, Common.NumberTime(tm), msgid))
}

func (this *Redis) GetLazyMsg(set string) ([]string, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Strings(rc.Do("ZRANGEBYSCORE", set, 0, Common.NumberNow()))
}

// 群组消息，使用有序集合ZSET，已经发送过的群消息的用户使用SET标记
// 保存新的群组消息，返回添加成功的消息数量
func (this *Redis) NewGroupMsg(key, msgid string, ttl int) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	// 创建标记集合并设置其生存周期为消息周期的2倍
	rc.Do("SADD", msgid, "0")
	rc.Do("EXPIRE", msgid, ttl*2)

	// 以消息的生命终点时间为score，添加到群组消息有序集合
	score := Common.NumberTime(time.Now().Add(time.Second * time.Duration(ttl)))
	return redis.Int64(rc.Do("ZADD", key, score, msgid))
}

// 标记发送过群组消息的用户，返回标记过的数量0|1
func (this *Redis) MarkGroupMsg(reject bool, userid int64, userFlag, msgid string) (int64, error) {
	if userid > 0 {
		defer this.MarkUserMsg(reject, userid, msgid)
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	// 若TTL不大于0，则该集合大限已到，不用标记了
	ttl, err := redis.Int64(rc.Do("TTL", msgid))
	if ttl <= 0 || err != nil {
		return 0, err
	}

	// 为了防止key泄露，重新设置TTL,保证它能到期删除
	defer rc.Do("EXPIRE", msgid, ttl)
	return redis.Int64(rc.Do("SADD", msgid, userFlag))
}

// 获取用户需要发送的所有群组消息
func (this *Redis) GetGroupMsg(key, userFlag string) ([]string, error) {
	if len(userFlag) == 0 {
		return nil, nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	// 取所有生存期内的群组消息的ID
	score := Common.NumberTime(time.Now())
	msgs, err := redis.Strings(rc.Do("ZRANGEBYSCORE", key, score, "+inf"))
	if err != nil {
		return nil, err
	}

	// 统计用户没有收到过的群组消息集合
	var ret []string
	for _, msg := range msgs {
		c, err := redis.Int64(rc.Do("SISMEMBER", msg, userFlag))
		if err != nil {
			return nil, err
		}

		if c == 0 {
			ret = append(ret, msg)
		}
	}

	return ret, nil
}

// 删除某等待发送的消息
func (this *Redis) DeleteMsg(key, msgid string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Int64(rc.Do("ZREM", key, msgid))
}

// 获取某群组消息的生命周期
func (this *Redis) IsGroupMsgExist(key, msgid string) (bool, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	score, err := rc.Do("ZSCORE", key, msgid)
	if err != nil {
		return false, err
	}
	if score == nil {
		return false, nil
	}

	return true, nil
}

// 设备消息，使用有序集合ZSET
// 保存新设备消息，返回添加成功的消息数量
func (this *Redis) NewDeviceMsg(devicekey string, ttl int, msgid string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	// 以消息的生命终点时间为score，添加到以devicekey为键的有序集合
	end := time.Now().Add(time.Second * time.Duration(ttl))
	return redis.Int64(rc.Do("ZADD", devicekey, Common.NumberTime(end), msgid))
}

// 标记发送过设备消息，返回标记过的消息数量0|1
func (this *Redis) MarkDeviceMsg(devicekey, msgid string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Int64(rc.Do("ZREM", devicekey, msgid))
}

// 获取未过期的待发送设备消息的ID
func (this *Redis) GetDeviceMsg(devicekey string) ([]string, error) {
	if len(devicekey) == 0 {
		return nil, nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	score := Common.NumberTime(time.Now())
	return redis.Strings(rc.Do("ZRANGEBYSCORE", devicekey, score, "+inf"))
}

// 用户消息，使用有序集合ZSET
// 保存新用户消息，返回添加成功的消息数量
func (this *Redis) NewUserMsg(userid int64, ttl int, msgid string) (int64, error) {
	if userid == 0 {
		return 0, nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	// 以消息的生命终点时间为score，添加到以用户为键的有序集合
	end := time.Now().Add(time.Second * time.Duration(ttl))
	return redis.Int64(rc.Do("ZADD", userid, Common.NumberTime(end), msgid))
}

// 标记发送过用户消息，返回标记过的消息数量0|1
func (this *Redis) MarkUserMsg(reject bool, userid int64, msgid string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	score := Common.NumberTime(time.Now())
	if reject {
		rc.Do("ZADD", fmt.Sprintf("%v_rejected", userid), score, msgid)
	} else {
		rc.Do("ZADD", fmt.Sprintf("%v_pushed", userid), score, msgid)
	}

	return redis.Int64(rc.Do("ZREM", userid, msgid))
}

// 获取已发送用户消息，返回已发送、已拒绝，已超时的消息ID
func (this *Redis) GetPushedUserMsg(userid int64) (send []string, rej []string, out []string) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	send, _ = redis.Strings(rc.Do("ZRANGE", fmt.Sprintf("%v_pushed", userid), 0, -1))
	rej, _ = redis.Strings(rc.Do("ZRANGE", fmt.Sprintf("%v_rejected", userid), 0, -1))

	score := Common.NumberTime(time.Now())
	out, _ = redis.Strings(rc.Do("ZRANGEBYSCORE", userid, "-inf", score))
	return
}

// 获取未过期的待发送用户消息的ID
func (this *Redis) GetUserMsg(userid int64) ([]string, error) {
	if userid == 0 {
		return nil, nil
	}

	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	score := Common.NumberTime(time.Now())
	return redis.Strings(rc.Do("ZRANGEBYSCORE", userid, score, "+inf"))
}

// 消息计数器,使用HASH表
// 获取所有Ack过的消息
func (this *Redis) GetAckedMsg(hashtable string) []string {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	sa, _ := redis.Strings(rc.Do("HKEYS", hashtable))
	return sa
}

// 增加发送过消息计数
func (this *Redis) AddMsgAck(hashtable, msgid string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	return redis.Int64(rc.Do("HINCRBY", hashtable, msgid, 1))
}

// 获取计数器值
func (this *Redis) GetMsgAck(hashtable, msgid string) int64 {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	c, _ := redis.Int64(rc.Do("HGET", hashtable, msgid))
	return c
}

// 重置计数器
func (this *Redis) ResetMsgAck(hashtable, msgid string, count int64) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	if this.GetMsgAck(hashtable, msgid) <= count {
		rc.Do("HDEL", hashtable, msgid)
	} else {
		rc.Do("HINCRBY", hashtable, msgid, -count)
	}
}

// 周期性或临时性调用清理方法
// 删除过期的消息，返回删除的消息数量，这个操作可能很耗时
// 参数为过期后仍然继续存储的时间（秒）
func (this *Redis) ClearOutDateMsg(keeptime int) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	keys, err := redis.Strings(rc.Do("KEYS", "*"))
	if err != nil {
		return 0, err
	}

	var count int64
	td := -1 * time.Second * time.Duration(keeptime)
	score := Common.NumberTime(time.Now().Add(td))
	for _, key := range keys {
		c, e := redis.Int64(rc.Do("ZREMRANGEBYSCORE", key, 0, score))
		if e == nil {
			count += c
		}
	}

	return count, nil
}

const limit_TM_FMT = "Limit-20060102"
const key_OFFICIAL = "Official-"

// 标记一条消息为官方消息
func (this *Redis) MarkOfficialMsg(msgid string, ttl int64) error {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	_, err := rc.Do("SETEX", key_OFFICIAL+msgid, ttl*10, msgid)
	return err
}

// 根据MsgID判断是不是官方消息
func (this *Redis) IsOfficialMsg(msgid string) bool {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	s, err := redis.String(rc.Do("GET", key_OFFICIAL+msgid))
	return err == nil && s == msgid
}

// 标记发送过官方消息的设备，返回标记过的数量0|1
func (this *Redis) MarkOfficialDevice(devicekey string) (int64, error) {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	set := time.Now().Format(limit_TM_FMT)
	defer rc.Do("EXPIRE", set, 10*24*3600)
	return redis.Int64(rc.Do("SADD", set, devicekey))
}

// 通过设备1天和7天收到的官方消息数量来决定它可不可以接受下一个消息
func (this *Redis) FullOfficialDevice(devicekey string) bool {
	defer Common.CheckPanic()
	rc := this.pool.Get()
	defer rc.Close()

	tm := time.Now()
	today := tm.Format(limit_TM_FMT)
	count, err := redis.Int64(rc.Do("SISMEMBER", today, devicekey))
	if err != nil {
		return true
	}

	if count > 0 {
		return true
	}

	for i := -1; i > -7; i-- {
		day := tm.Add(time.Hour * 24 * time.Duration(i)).Format(limit_TM_FMT)
		c, err := redis.Int64(rc.Do("SISMEMBER", day, devicekey))
		if err != nil {
			return true
		}

		count += c
		if count >= 3 {
			return true
		}
	}

	return false
}
