package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/luci/go-render/render"
)

var pool *redis.Pool

const ARTICLETIME = 7 * 86400
const VOTESCORE = 432
const PERPAGE = 25

//结构体后面必须要带注释，不然redis无法解析结构
type ArticleInfo struct {
	Title  string `redis:"title"`
	Link   string `redis:"link"`
	Poster string `redis:"poster"`
	Time   string `redis:"time"`
	Votes  int64  `redis:"votes"`
}

type ArticleInfoResp struct {
	ArticleInfoList []*ArticleInfo
	ErrCode         string
}

func init() {
	pool = &redis.Pool{
		MaxIdle:     16,
		MaxActive:   1024,
		IdleTimeout: 300,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
	}
}

func HandlePostArticle(userId string, title string, link string) (string, error) {
	conn := pool.Get()
	defer conn.Close()

	//设置投票序列
	articleId := "00000"
	values, err := redis.Int(conn.Do("INCR", "article:"))
	if err != nil {
		fmt.Println(err)
		return articleId, err
	}

	articleId = strconv.Itoa(values)
	voteId := "voted:" + articleId
	postId := "article:" + articleId
	_, err = conn.Do("SADD", voteId, userId)
	if err != nil {
		fmt.Println("SADD articleId failed")
		return postId, err
	}
	_, err = conn.Do("EXPIRE", voteId, ARTICLETIME)

	//发布文章信息
	postTime := time.Now().Unix()
	_, err = conn.Do("HMSET", postId, "title", title, "link", link, "poster",
		userId, "time", postTime, "votes", 1)

	//更新文章发布时间和分数
	_, err = conn.Do("ZADD", "score:", postTime+VOTESCORE, postId)
	_, err = conn.Do("ZADD", "time:", postTime, postId)

	fmt.Println(postId, " has been post")
	return postId, err
}

func HandleVoteArticle(articleId string, userId string) error {
	conn := pool.Get()
	defer conn.Close()

	cutoff := time.Now().Unix() - ARTICLETIME
	artId := "article:" + articleId
	postTime, err := redis.Int64(conn.Do("ZSCORE", "time:", artId))
	if err != nil {
		fmt.Println("ZSCORE err ", err)
		return err
	}

	fmt.Println("postTime ", render.Render(postTime))

	if postTime < cutoff {
		fmt.Println("vote time has pass")
		return err
	}

	//如果已经投票返回0，未投票返回1
	if values, _ := redis.Int(conn.Do("SADD", "voted:"+articleId, userId)); values == 1 {
		//第一次投票增加分数和投票数目，如果已经投过则不投
		conn.Do("ZINCRBY", "score:", VOTESCORE, artId)
		conn.Do("HINCRBY", artId, "votes", 1)
	} else {
		fmt.Println("ZSDD err ", err)
	}
	return err
}

//key表示存储文章的key，是按分数或者按发布时间获取
func HandleGetArticle(page int, key string) (*ArticleInfoResp, error) {
	resp := &ArticleInfoResp{}
	conn := pool.Get()
	defer conn.Close()

	start := (page - 1) * PERPAGE
	end := start + PERPAGE - 1
	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, end))
	if err != nil {
		fmt.Println(err)
		resp.ErrCode = "GETARTID_FAILED"
		return resp, err
	}

	var idList []string
	if err = redis.ScanSlice(values, &idList); err != nil {
		fmt.Println(err)
		resp.ErrCode = "GETARTID_FAILED"
		return resp, err
	}

	fmt.Println("nameList ", render.Render(idList))

	for _, id := range idList {
		articleInfo := &ArticleInfo{}
		info, _ := redis.Values(conn.Do("HGETALL", id))
		fmt.Println("articleInfos ", render.Render(info))
		if err = redis.ScanStruct(info, articleInfo); err == nil {
			resp.ArticleInfoList = append(resp.ArticleInfoList, articleInfo)
		}
	}

	resp.ErrCode = "SUCCESS"
	fmt.Println("resp", render.Render(resp))
	return resp, nil
}

func HandleAddRemoveGroups(articleId string, addList string, rmList string) []string {
	conn := pool.Get()
	defer conn.Close()

	var ret []string
	article := "article:" + articleId
	for _, group := range strings.Split(addList, ",") {
		_, err := conn.Do("SADD", "group:"+group, article)
		if err != nil {
			fmt.Println("add to group "+group+" failed ", err)
			ret = append(ret, "Add to group "+group+" failed")
		} else {
			ret = append(ret, "Add to group "+group+" SUCCESS")
		}
	}

	for _, group := range strings.Split(rmList, ",") {
		_, err := conn.Do("SREM", "group:"+group, article)
		if err != nil {
			fmt.Println("rm from group "+group+" failed ", err)
			ret = append(ret, "rm from group "+group+" failed ")
		} else {
			ret = append(ret, "rm from group "+group+" SUCCESS")
		}
	}

	return ret
}

//key 可以是时间排序的文章也可以是打分排名的文章
func HandleGetGroupArticles(group string, page int, key string) (*ArticleInfoResp, error) {
	resp := &ArticleInfoResp{}
	conn := pool.Get()
	defer conn.Close()

	tmpKey := "score:" + group
	values, err := redis.Int64(conn.Do("EXISTS", tmpKey))

	if err != nil {
		fmt.Println("GetGroupArticles failed ", err)
		resp.ErrCode = "GETGROUPART_ERROR"
		return resp, err
	}

	if values == 0 {
		conn.Do("ZINTERSTORE", key, 2, "group:"+group, key)
	}
	return HandleGetArticle(page, key)
}

func PostArticle(w http.ResponseWriter, r *http.Request) {
	userId := r.PostFormValue("userId")
	title := r.PostFormValue("title")
	link := r.PostFormValue("link")
	fmt.Println("request ", userId, title, link)
	resp := make(map[string]string)
	articleId, err := HandlePostArticle(userId, title, link)
	if err != nil {
		resp[articleId] = "POST_ERROR"
	} else {
		resp[articleId] = "SUCCESS"
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(data))
}

func VoteArticle(w http.ResponseWriter, r *http.Request) {
	articleId := r.PostFormValue("articleId")
	userId := r.PostFormValue("userId")
	fmt.Println("request ", userId, articleId)
	resp := make(map[string]string)
	err := HandleVoteArticle(articleId, userId)
	if err != nil {
		resp[articleId] = "VOTE_ERROR"
	} else {
		resp[articleId] = "SUCCESS"
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(data))
}

func GetArticle(w http.ResponseWriter, r *http.Request) {
	reqValues, _ := url.ParseQuery(r.URL.RawQuery)
	page, _ := strconv.Atoi(reqValues["page"][0])
	key := reqValues["key"][0]
	fmt.Println("request ", key, page)
	resp, err := HandleGetArticle(page, key)
	if err != nil {
		fmt.Println("HandleGetArticle failed")
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(data))
}

func AddRemoveGroups(w http.ResponseWriter, r *http.Request) {
	reqValues, _ := url.ParseQuery(r.URL.RawQuery)
	var addList string
	var rmList string
	if val, ok := reqValues["add"]; ok {
		addList = val[0]
	}

	if val, ok := reqValues["rm"]; ok {
		rmList = val[0]
	}

	id := reqValues["id"][0]
	fmt.Println("request ", addList, rmList, id)
	resp := HandleAddRemoveGroups(id, addList, rmList)
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(data))
}

func GetGroupArticles(w http.ResponseWriter, r *http.Request) {
	reqValues, _ := url.ParseQuery(r.URL.RawQuery)
	page, _ := strconv.Atoi(reqValues["page"][0])
	key := reqValues["key"][0]
	group := reqValues["group"][0]
	fmt.Println("request ", group, key, page)
	resp, err := HandleGetGroupArticles(group, page, key)
	if err != nil {
		fmt.Println("HandleGetArticle failed")
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(data))
}

func main() {
	http.HandleFunc("/post_article", PostArticle)
	http.HandleFunc("/vote_article", VoteArticle)
	http.HandleFunc("/get_article", GetArticle)
	http.HandleFunc("/add_rm_article", AddRemoveGroups)
	http.HandleFunc("/get_group_article", GetGroupArticles)

	fmt.Println("http start to listen")
	err := http.ListenAndServe("10.1.22.115:9090", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
