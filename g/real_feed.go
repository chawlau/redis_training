package redi

import (
	"common"
	"encoding/json"
	"fmt"
	"modules/feed_data_manager/g"
	"protocols"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/luci/go-render/render"
)

func contructRealFeedKey(stockTiny *protocols.StockTiny) string {
	return fmt.Sprintf("RealFeed:%d:%s", stockTiny.StockType, stockTiny.StockId)
}

func contructMarketDepthKey(stockTiny *protocols.StockTiny) string {
	return fmt.Sprintf("MarketDepth:%d:%s", stockTiny.StockType, stockTiny.StockId)
}

func contructHSMarketDetailKey(hsMarketMic string) string {
	return fmt.Sprintf("HSMarket:%s", hsMarketMic)
}

func StoreRealFeeds(realFeeds []*protocols.RealFeed, lastQueryTimeFlag bool) error {
	// send to redis
	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()
	var returnErr error = nil

	for _, realFeed := range realFeeds {
		redisKey := contructRealFeedKey(realFeed.StockTiny)
		realFeedJson, err := json.Marshal(realFeed)
		if err != nil {
			common.Logger.Error("json.Marshal failed. err:", err.Error(), " realFeed:", render.Render(realFeed))
			continue
		}

		var cmd string
		if lastQueryTimeFlag {
			cmd = fmt.Sprintln("HMSET", redisKey, "RealFeed", realFeedJson, "DataUpdateTime", time.Now().Unix(), "LastQueryTime", time.Now().Unix())
			_, err = redisConn.Do("HMSET", redisKey, "RealFeed", realFeedJson, "DataUpdateTime", time.Now().Unix(), "LastQueryTime", time.Now().Unix())
		} else {
			cmd = fmt.Sprintln("HMSET", redisKey, "RealFeed", realFeedJson, "DataUpdateTime", time.Now().Unix())
			_, err = redisConn.Do("HMSET", redisKey, "RealFeed", realFeedJson, "DataUpdateTime", time.Now().Unix())
		}
		if err != nil {
			common.Logger.Warn("redis failed. ", cmd, " err:", err.Error())
			returnErr = err
		}
	}
	return returnErr
}

func GetRealFeed(stockTinys []*protocols.StockTiny, lastQueryTime bool) (realFeeds map[protocols.StockTiny]*protocols.RealFeed, err error) {
	realFeeds = make(map[protocols.StockTiny]*protocols.RealFeed)

	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()

	for _, stockTiny := range stockTinys {
		redisKey := contructRealFeedKey(stockTiny)
		realFeed, err := getRealFeedInner(redisConn, redisKey, lastQueryTime)
		if err != nil {
			common.Logger.Warn("getRealFeedInner error.", redisKey, lastQueryTime, realFeed, err)
			continue
		}
		realFeeds[*stockTiny] = &realFeed
	}
	return realFeeds, err
}

func getRealFeedInner(redisConn redis.Conn, realFeedKey string, lastQueryTimeFlag bool) (realFeed protocols.RealFeed, err error) {
	realFeed = protocols.RealFeed{}
	values, err := redis.Values(redisConn.Do("HMGET", realFeedKey, "RealFeed", "LastQueryTime"))
	if err != nil {
		common.Logger.Warn("HMGET", realFeedKey, " failed.", err)
		return realFeed, err
	}

	var realFeedJson string
	var lastQueryTime int64
	_, err = redis.Scan(values, &realFeedJson, &lastQueryTime)
	if err != nil {
		common.Logger.Warn("HGET", realFeedKey, " failed.", err)
		return realFeed, err
	}

	err = json.Unmarshal([]byte(realFeedJson), &realFeed)
	if err != nil {
		common.Logger.Warn("Unmarshal failed.", err, "realFeedKey", realFeedKey, "realFeedJson:", realFeedJson)
		return realFeed, err
	}
	realFeed.LastQueryTimestamp = lastQueryTime

	if lastQueryTimeFlag {
		// 更新最后查询时间戳
		lastQueryTime = time.Now().Unix()
		_, err = redisConn.Do("HSET", realFeedKey, "LastQueryTime", lastQueryTime)
		if err != nil {
			common.Logger.Error("redis failed. when HSET", realFeedKey, "LastQueryTime", lastQueryTime)
		}
	}

	return realFeed, nil
}

func ScanRealFeeds(scanType protocols.ScanType, index int32, maxCount int32) (realFeeds []*protocols.RealFeed, newIndex int, err error) {
	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()

	matchRegular := "RealFeed:*"
	if scanType == protocols.HKStock {
		matchRegular = "RealFeed:10:*"
	} else if scanType == protocols.USStock {
		matchRegular = "RealFeed:11:*"
	}

	values, err := redis.Values(redisConn.Do("SCAN", index, "MATCH", matchRegular, "Count", maxCount))
	if err != nil {
		common.Logger.Error("redis failed. when SCAN", index, "MATCH", matchRegular, "Count", maxCount, err)
		return nil, 0, err
	}

	var keys []string
	_, err = redis.Scan(values, &newIndex, &keys)
	if err != nil {
		common.Logger.Error("redis scan result failed. Scan", err)
	}

	for _, realFeedKey := range keys {
		realFeed, err := getRealFeedInner(redisConn, realFeedKey, false)
		if err != nil {
			common.Logger.Error("getRealFeedInner failed. ", realFeedKey, err)
		}
		realFeeds = append(realFeeds, &realFeed)
	}
	return realFeeds, newIndex, nil
}

func DeleteRealFeeds(stockTinys []*protocols.StockTiny) error {
	// send to redis
	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()

	var returnErr error = nil

	// 考虑批量 TODO(xf)
	for _, stockTiny := range stockTinys {
		redisKey := contructRealFeedKey(stockTiny)
		_, err := redisConn.Do("DEL", redisKey)
		if err != nil {
			common.Logger.Error("redisConn.Do failed. DEL", redisKey, " err:", err.Error())
			returnErr = err
		}
	}
	return returnErr
}

func StoreMarketDepthes(marketDepthes map[protocols.StockTiny]*protocols.MarketDepth) error {
	// send to redis
	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()
	var returnErr error = nil

	for stockTiny, marketDepth := range marketDepthes {
		redisKey := contructMarketDepthKey(&stockTiny)
		marketDepthJson, err := json.Marshal(marketDepth)
		if err != nil {
			common.Logger.Error("json.Marshal failed. err:", err.Error(), " marketDepth:", render.Render(marketDepth))
			continue
		}

		dataUpdateTime := time.Now().Unix()
		_, err = redisConn.Do("HMSET", redisKey, "MarketDepth", marketDepthJson, "DataUpdateTime", dataUpdateTime)
		if err != nil {
			common.Logger.Error("redisConn.Do failed. HMSET", redisKey, "MarketDepth", marketDepthJson, "DataUpdateTime", dataUpdateTime,
				" err:", err.Error())
			returnErr = err
		}
	}
	return returnErr
}

func GetMarketDepthes(stockTinys []*protocols.StockTiny) (marketDepthes map[protocols.StockTiny]*protocols.MarketDepth, err error) {
	marketDepthes = make(map[protocols.StockTiny]*protocols.MarketDepth)

	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()

	for _, stockTiny := range stockTinys {
		redisKey := contructMarketDepthKey(stockTiny)
		marketDepthJson, err := redis.String(redisConn.Do("HGET", redisKey, "MarketDepth"))
		if err != nil || marketDepthJson == "" {
			common.Logger.Warn("HGET", redisKey, " failed.", err.Error()) // TODO(xf)
			continue
		}

		marketDepth := protocols.MarketDepth{}
		err = json.Unmarshal([]byte(marketDepthJson), &marketDepth)
		if err != nil {
			common.Logger.Error("Unmarshal failed.", err.Error(), "marketDepthJson:", marketDepthJson)
			continue
		}
		marketDepthes[*stockTiny] = &marketDepth
	}
	return marketDepthes, err
}

func StoreHSMaketDetail(hsMarketMic string, hsMarketDetail string) error {
	// send to redis
	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()
	var returnErr error = nil

	redisKey := contructHSMarketDetailKey(hsMarketMic)
	dataUpdateTime := time.Now().Unix()
	_, err := redisConn.Do("HMSET", redisKey, "MarketDetail", hsMarketDetail, "DataUpdateTime", time.Now().Unix())
	if err != nil {
		common.Logger.Error("redisConn.Do failed. HMSET", redisKey, "MarketDetail", hsMarketDetail, "DataUpdateTime", dataUpdateTime,
			" err:", err.Error())
		returnErr = err
	}
	return returnErr
}

func GetHSMaketDetail(hsMarketMic string) (hsMarketDetail string, lasteUpdateTimeMS int64, err error) {
	redisConn := g.RedisConnPool.Get()
	defer redisConn.Close()

	redisKey := contructHSMarketDetailKey(hsMarketMic)

	reply, err := redis.MultiBulk(redisConn.Do("HMGET", redisKey, "MarketDetail", "DataUpdateTime"))
	if err != nil {
		common.Logger.Warn("HMGET", redisKey, " failed.", err.Error()) // TODO(xf)
		return hsMarketDetail, lasteUpdateTimeMS, err
	}

	for field, value := range reply {
		if field == 0 {
			hsMarketDetail, err = redis.String(value, nil)
		} else if field == 1 {
			lasteUpdateTimeMS, err = redis.Int64(value, nil)
		}
	}
	return hsMarketDetail, lasteUpdateTimeMS, err
}
