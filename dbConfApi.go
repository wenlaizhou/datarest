package dbrest

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/wenlaizhou/middleware"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type SqlApi struct {
	Result      int
	Path        string
	Transaction bool
	Sqls        []SqlConf
	Params      map[string]string
	PassError   bool     // 是否忽略错误, 多条sql语句时, 当其中一条出错, 会终止之后的执行
	Must        []string // 必须不为空的参数列表, 使用,分割 例如: <must>asd,ads,das</must>
}

type SqlConf struct {
	HasSql    bool
	Type      string
	Table     string
	SqlOrigin string
	RParams   []SqlParam
	Params    []SqlParam
	Id        string
}

type SqlParam struct {
	Type  int
	Key   string
	Value interface{}
	// Id   string
}

const (
	Post   = 0 // ${}
	Result = 1 // @{} result结果只能具有id类型
	Param  = 2
	// Replace = 2 //#{}
	// guid : {{guid}}
)

const (
	Insert = "insert"
	Select = "select"
	Update = "update"
	Delete = "delete"
)

const (
	Normal  = 0
	Combine = 1
)

// 六种类型参数
// 1: post sql参数
// 2: result sql参数
// 3: post replace参数
// 4: result replace参数
// 5: param sql参数
// 6: param replace参数

var postReg = regexp.MustCompile("\\$\\{(.*?)\\}")
var resultReplaceReg = "#\\{%s\\.(.*?)\\}"
var resultReg = "$\\{%s\\.(.*?)\\}"
var replaceReg = regexp.MustCompile("#\\{(.*?)\\}")
var sqlApis = make(map[string]SqlApi)

// 初始化数据库api配置
//
// 可重复更新配置
//
// 配置文件路径
func InitSqlConfApi(filePath string) {
	apiConf := middleware.LoadXml(filePath)
	apiElements := apiConf.FindElements("//sqlApi")
	for _, apiEle := range apiElements {
		sqlIds := make([]string, 0)
		sqlApi := *new(SqlApi)
		sqlApi.Transaction = apiEle.SelectAttrValue("transaction", "") == "true"
		sqlApi.PassError = apiEle.SelectAttrValue("passError", "") == "true"
		sqlApi.Path = apiEle.SelectAttrValue("path", "")

		sqlApi.Sqls = make([]SqlConf, 0)

		sqlApi.Params = make(map[string]string)
		for _, paramEle := range apiEle.FindElements(".//param") {
			sqlApi.Params[paramEle.SelectAttrValue("key", "")] = paramEle.SelectAttrValue("value", "")
		}

		for i, sqlEle := range apiEle.FindElements(".//sql") {
			oneSql := new(SqlConf)
			oneSql.Table = sqlEle.SelectAttrValue("table", "")
			oneSql.Id = sqlEle.SelectAttrValue("id", strconv.Itoa(i))
			sqlIds = append(sqlIds, oneSql.Id)
			sqlStr := strings.TrimSpace(sqlEle.Text())
			if len(sqlStr) <= 0 {
				oneSql.HasSql = false
				oneSql.Type = sqlEle.SelectAttrValue("type", "")
				if !oneSql.HasSql && len(oneSql.Type) <= 0 {
					// 配置错误
				}
			} else {
				oneSql.HasSql = true
				// 参数计算
				oneSql.SqlOrigin, oneSql.RParams, oneSql.Params = parseSql(sqlStr)
			}
			sqlApi.Sqls = append(sqlApi.Sqls, *oneSql)
		}

		for _, mustEle := range apiEle.FindElements(".//must") {
			mustContent := mustEle.Text()
			if len(mustContent) > 0 && len(strings.TrimSpace(mustContent)) > 0 {
				mustContent = strings.TrimSpace(mustContent)
				mustParams := strings.Split(mustContent, ",")
				for _, mustParam := range mustParams {
					sqlApi.Must = append(sqlApi.Must, mustParam)
				}
			}
		}

		// 注册每个配置对应的接口服务
		sqlApis[sqlApi.Path] = sqlApi
		registerSqlConfApi(sqlApi)
	}

}

func ExecSqlConfApi(params map[string]interface{}, path string) ([]map[string]string, error) {
	sqlApi, ok := sqlApis[path]
	sqlApiParams := make(map[string]string)
	if !ok {
		return nil, errors.New("没有该路径sqlApi配置")
	}

	// 必须具有参数列表

	// <must>asd, asd, asd, asd</must>

	// 处理guid
	for k, v := range sqlApi.Params {
		sqlApiParams[k] = v
		if v == "{{guid}}" {
			sqlApiParams[k] = middleware.Guid()
		}
	}

	session := dbApiInstance.GetEngine().NewSession()
	defer session.Close()
	if sqlApi.Transaction {
		session.Begin()
	}
	result := make([]map[string]string, 0)

	for _, sqlInstance := range sqlApi.Sqls {
		if sqlInstance.HasSql {
			oneSqlRes, err := exec(*session, sqlInstance, params, sqlApiParams)
			if middleware.ProcessError(err) {
				if !sqlApi.PassError {
					if sqlApi.Transaction {
						middleware.ProcessError(session.Rollback())
					}
					return result, err
				}
			}
			if a, b := oneSqlRes.(sql.Result); b {
				if id, err := a.LastInsertId(); err == nil && len(sqlInstance.Id) > 0 {
					sqlApiParams[fmt.Sprintf("%s.id", sqlInstance.Id)] = fmt.Sprintf("%v", id)
				}
			}
			if a, b := oneSqlRes.([]map[string]string); b {
				result = append(result, a...)
			}
			continue
		}

		// table 中含有参数类型数据, 进行处理
		if postReg.MatchString(sqlInstance.Table) {
			tableParam := postReg.FindAllStringSubmatch(sqlInstance.Table, -1)
			tableParamName := tableParam[0][1]
			if _, ok := params[tableParamName]; ok {
				sqlInstance.Table = params[tableParamName].(string)
			}
			if _, ok := sqlApiParams[tableParamName]; ok {
				sqlInstance.Table = sqlApiParams[tableParamName]
			}
		}

		switch {
		case "insert" == sqlInstance.Type:
			id, err := doInsert(*session, sqlInstance, params, sqlApiParams)
			if middleware.ProcessError(err) {
				if !sqlApi.PassError {
					if sqlApi.Transaction {
						middleware.ProcessError(session.Rollback())
					}
					return result, err
				}
			}
			// 增加id配置处理
			sqlApiParams[fmt.Sprintf("%s.id", sqlInstance.Id)] = fmt.Sprintf("%v", id)
			break
		case "select" == sqlInstance.Type:
			oneSqlRes, err := doSelect(*session, sqlInstance, params, sqlApiParams)
			if middleware.ProcessError(err) {
				if !sqlApi.PassError {
					if sqlApi.Transaction {
						middleware.ProcessError(session.Rollback())
					}
					return result, err
				}
			}
			result = append(result, oneSqlRes...)
			break
		case "update" == sqlInstance.Type:
			_, err := doUpdate(*session, sqlInstance, params)
			if middleware.ProcessError(err) {
				if !sqlApi.PassError {
					if sqlApi.Transaction {
						middleware.ProcessError(session.Rollback())
					}
					return result, err
				}
			}
			break
		case "delete" == sqlInstance.Type:
			err := doDelete(*session, sqlInstance, params)
			if middleware.ProcessError(err) {
				if !sqlApi.PassError {
					if sqlApi.Transaction {
						middleware.ProcessError(session.Rollback())
					}
					return result, err
				}
			}
			break
		}

	}
	if len(sqlApiParams) > 0 {
		result = append(result, sqlApiParams)
	}

	if sqlApi.Transaction {
		middleware.ProcessError(session.Commit())
	}
	return result, nil
}

func registerSqlConfApi(sqlApi SqlApi) {
	if len(sqlApi.Path) <= 0 {
		log.Printf("sqlApi注册失败 : %#v 没有服务路径", sqlApi)
		return
	}
	log.Printf("注册sql api服务: %#v", sqlApi)
	middleware.RegisterHandler(sqlApi.Path,
		func(context middleware.Context) {
			sqlApi := sqlApi
			jsonData, err := context.GetJSON()
			if middleware.ProcessError(err) {
				jsonData = make(map[string]interface{})
			}
			log.Printf("sql-api 获取调用: %s", sqlApi.Path)
			log.Printf("参数: %v", jsonData)
			if len(sqlApi.Must) > 0 {
				for _, mustParam := range sqlApi.Must {
					if v, ok := jsonData[mustParam]; !ok {
						_ = context.ApiResponse(-1, fmt.Sprintf("%s为必须参数", mustParam), nil)
						return
					} else {
						if v == nil {
							_ = context.ApiResponse(-1, fmt.Sprintf("%s为必须参数", mustParam), nil)
							return
						}
					}
				}
			}
			res, err := ExecSqlConfApi(jsonData, sqlApi.Path)
			if middleware.ProcessError(err) {
				_ = context.ApiResponse(-1, err.Error(), nil)
				return
			} else {
				_ = context.ApiResponse(0, "", res)
				return
			}
			return

		})
}
