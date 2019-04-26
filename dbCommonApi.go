package dbrest

import (
	"encoding/json"
	"fmt"
	"github.com/go-xorm/core"
	"github.com/wenlaizhou/middleware"
	"strings"
)

var Tables []*core.Table

var tableMetas map[string]core.Table

var Config middleware.Config

var inited = false

// 统一入口
// 初始化数据库连接
// 调用该方法可重复更新配置, 重新创建连接
//
// 配置:
// {
// 	"db.host" : "",
// 	"db.port" : 3306,
// 	"db.user" : "",
// 	"db.password" : "",
// 	"db.database" : ""
// }
func InitDbApi(conf middleware.Config) {

	Config = conf
	initEngine()
	tablesMeta, err := dbApiInstance.GetEngine().DBMetas()
	if middleware.ProcessError(err) {
		return
	}

	tableMetas = make(map[string]core.Table)

	Tables = make([]*core.Table, 0)

	for _, tableMeta := range tablesMeta {
		tableMeta := tableMeta
		Tables = append(Tables, tableMeta)
		tableMetas[tableMeta.Name] = *tableMeta
		registerTableCommonApi(*tableMeta)
	}
	registerTables()
	if middleware.ProcessError(err) {
		return
	}
	// 注册sql接口
	middleware.RegisterHandler(fmt.Sprintf("/sql"),
		func(context middleware.Context) { // 安全
			jsonParam, err := context.GetJSON()
			if middleware.ProcessError(err) {
				_ = context.ApiResponse(-1, "参数错误", nil)
				return
			}
			sql := jsonParam["sql"]
			if sql == nil {
				_ = context.ApiResponse(-1, "参数错误", nil)
				return
			}
			sqlStr, ok := sql.(string)
			if !ok {
				_ = context.ApiResponse(-1, "参数错误", nil)
				return
			}

			sqlStr = strings.TrimSpace(sqlStr)

			if len(sqlStr) <= 0 {
				_ = context.ApiResponse(-1, "参数不包含sql", nil)
				return
			}

			if strings.Contains(strings.ToUpper(sqlStr), "DELETE") {
				_ = context.ApiResponse(-1, "sql参数中不允许出现delete", nil)
				return
			}

			logSql(context, sqlStr, nil)
			res, err := dbApiInstance.GetEngine().QueryString(sqlStr)
			if !middleware.ProcessError(err) {
				Logger.InfoF("%s\n, %s\n, %s\n",
					context.RemoteAddr(),
					string(context.Request.UserAgent()),
					sqlStr)
				_ = context.ApiResponse(0, "", res)
			} else {
				_ = context.ApiResponse(-1, err.Error(), res)
			}

		})
}

func registerTables() {
	middleware.RegisterHandler("/tables", func(context middleware.Context) {
		tablesBytes, _ := json.Marshal(Tables)
		tablesResult := string(tablesBytes)
		_ = context.JSON(tablesResult)
		return
	})
}

func registerTableCommonApi(tableMeta core.Table) {
	registerTableInsert(tableMeta)
	registerTableUpdate(tableMeta)
	registerTableSelect(tableMeta)
	registerTableDelete(tableMeta)
	registerTableCount(tableMeta)
	registerTableSchema(tableMeta)
}

func registerTableInsert(tableMeta core.Table) {
	middleware.RegisterHandler(fmt.Sprintf("%s/insert", tableMeta.Name),
		func(context middleware.Context) {
			params, err := context.GetJSON()
			if middleware.ProcessError(err) || len(params) <= 0 {
				_ = context.ApiResponse(-1, "参数错误", nil)
				return
			}
			Logger.InfoF("获取insert调用: %v", params)
			id, err := doInsert(*GetEngine().NewSession(), SqlConf{
				Id:    tableMeta.Name,
				Table: tableMeta.Name,
			}, params, nil)
			if err != nil {
				_ = context.ApiResponse(-1, err.Error(), nil)
				return
			}
			_ = context.ApiResponse(0, "", id)
		})
}

func registerTableDelete(tableMeta core.Table) {
	middleware.RegisterHandler(fmt.Sprintf("%s/delete", tableMeta.Name),
		func(context middleware.Context) {
			params, err := context.GetJSON()
			if err != nil || len(params) <= 0 {
				_ = context.ApiResponse(-1, "参数错误", nil)
				return
			}
			primaryValue, ok := params["id"]
			if !ok || primaryValue == nil {
				_ = context.ApiResponse(-1, "删除数据必须指定id值", nil)
				return
			}
			if len(tableMeta.PrimaryKeys) <= 0 {
				_ = context.ApiResponse(-1, "表不存在主键, 无法删除数据", nil)
				return
			}
			Logger.InfoF("获取delete调用: %v", params)
			primaryKey := tableMeta.PrimaryKeys[0]
			sql := fmt.Sprintf("delete from %s where %s = ?;", tableMeta.Name, primaryKey)
			res, err := dbApiInstance.GetEngine().Exec(sql, primaryValue)
			if !middleware.ProcessError(err) {
				logSql(context, sql, []interface{}{primaryValue})
				rowsAffected, err := res.RowsAffected()
				if !middleware.ProcessError(err) {
					_ = context.ApiResponse(0, "success", rowsAffected)
					return
				} else {
					_ = context.ApiResponse(-1, err.Error(), nil)
				}
			} else {
				_ = context.ApiResponse(-1, err.Error(), nil)
				return
			}
		})
}

func registerTableUpdate(tableMeta core.Table) {
	middleware.RegisterHandler(fmt.Sprintf("%s/update", tableMeta.Name),
		func(context middleware.Context) {
			params, err := context.GetJSON()
			if err != nil || len(params) <= 0 {
				_ = context.ApiResponse(-1, "参数错误", nil)
				return
			}
			Logger.InfoF("获取update调用: %v", params)
			res, err := doUpdate(*GetEngine().NewSession(), SqlConf{
				Table: tableMeta.Name,
			}, params)
			if err != nil {
				_ = context.ApiResponse(-1, err.Error(), nil)
				return
			} else {
				_ = context.ApiResponse(0, "success", res)
				return
			}
		})
}

func registerTableSelect(tableMeta core.Table) {
	middleware.RegisterHandler(fmt.Sprintf("%s/select", tableMeta.Name),
		func(context middleware.Context) {
			params, err := context.GetJSON()
			if err != nil {
				params = nil
			}
			Logger.InfoF("获取select调用: %v", params)
			res, err := doSelect(*GetEngine().NewSession(), SqlConf{
				Table:  tableMeta.Name,
				HasSql: false,
			}, params, nil)
			if middleware.ProcessError(err) {
				_ = context.ApiResponse(-1, err.Error(), nil)
				return
			}
			_ = context.ApiResponse(0, "", res)
			return
		})
}

func registerTableCount(tableMeta core.Table) {
	middleware.RegisterHandler(fmt.Sprintf("%s/count", tableMeta.Name),
		func(context middleware.Context) {
			params, err := context.GetJSON()
			if err != nil {
				params = nil
			}
			Logger.InfoF("获取select调用: %v", params)
			res, err := doCount(*GetEngine().NewSession(), SqlConf{
				Table:  tableMeta.Name,
				HasSql: false,
			}, params, nil)
			if middleware.ProcessError(err) {
				_ = context.ApiResponse(-1, err.Error(), nil)
				return
			}
			_ = context.ApiResponse(0, "", res)
			return
		})
}

func registerTableSchema(tableMeta core.Table) {
	middleware.RegisterHandler(fmt.Sprintf("%s/schema", tableMeta.Name),
		func(context middleware.Context) {
			_ = context.ApiResponse(0, "",
				tableMeta.Columns())
		})
}
