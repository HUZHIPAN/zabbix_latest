package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/nxadm/tail"
	"github.com/pelletier/go-toml/v2"
)

// 解析的配置文件参数
type ExportConfig struct {
	ExportDir   string   // zabbix ExportDir配置的实时导出目录
	FilesPrefix []string // 需要处理的导出文件前缀
	WatchWhence int      // 从哪里开始监听文件 0扫描文件全部 2从文件尾部开头监听

	DBType       string
	Host         string
	Port         string
	Username     string
	Password     string
	DatabaseName string
	TablePrefix  string

	LogPath           string
	SendWindowSize    [5]int
	SendWindowTimeout [5]int
	DBMaxOpenConn     int
}

type WatchFile struct {
	Name string
	Tail tail.Tail
}

var (
	cfg         ExportConfig
	loggerFile  *os.File
	listenFiles []string
	dbConn      *sql.DB
)

// 数据通道
var (
	messageValueType0 = make(chan *ItemRecordRow[float64], 10000) // 数据value_type为0的数据
	messageValueType1 = make(chan *ItemRecordRow[string], 10000)
	messageValueType2 = make(chan *ItemRecordRow[string], 10000)
	messageValueType3 = make(chan *ItemRecordRow[int64], 10000)
	messageValueType4 = make(chan *ItemRecordRow[string], 10000)

	logChan = make(chan string, 10000) // 日志通道
)

const HEART_BEAT = "" // 心跳信号

var (
	acceptTotalCount uint64 // 总接收数据数量
	sendSuccessCount uint64 // 发送成功数
	sendFailedCount  uint64 // 发送异常数
	skipRefreshCount uint64 // 跳过更新数量
)

type ItemRecordRow[T int64 | float64 | string] struct {
	itemid     int64
	clock      int
	value_type int8
	value      T
}

// value_type 0，1，2，3，4 对应的数据库字段，按下标顺序，不可更换
var valueFieldsMap []string = []string{"value_float", "value_str", "value_log", "value_uint", "value_text"}


func main() {

	loadConf()
	loadDatabase(true)
	checkHistoryTableExist()
	loadWatchExportFile(true) // 加速大概率事件远比优化小概率事件更能提高性能

	go registerLogger()
	go pollerSenderHeartbeat()

	go sender(messageValueType0, 0, cfg.SendWindowSize[0], int64(cfg.SendWindowTimeout[0]))
	go sender(messageValueType1, 1, cfg.SendWindowSize[1], int64(cfg.SendWindowTimeout[1]))
	go sender(messageValueType2, 2, cfg.SendWindowSize[2], int64(cfg.SendWindowTimeout[2]))
	go sender(messageValueType3, 3, cfg.SendWindowSize[3], int64(cfg.SendWindowTimeout[3]))
	go sender(messageValueType4, 4, cfg.SendWindowSize[4], int64(cfg.SendWindowTimeout[4]))

	select {}
}

// @brief：耗时统计函数
func TimeCost(identification string) func() {
	start := time.Now()
	return func() {
		tc := time.Since(start)
		fmt.Printf("%v ：time cost = %v\n", identification, tc)
	}
}

// 初始化配置
func loadConf() {
	file, err := os.ReadFile("./conf.ini")
	if err != nil {
		panic(err)
	}
	err = toml.Unmarshal([]byte(file), &cfg)
	if err != nil {
		panic(err)
	}

	err = initLoggerFile()
	if err != nil {
		panic(err)
	}

	if !inArray(cfg.DBType, []string{"mysql", "postgres"}) {
		panic("未知的数据库驱动：" + cfg.DBType)
	}
}

func initLoggerFile() error {
	var err error
	loggerFile, err = os.OpenFile(cfg.LogPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	return nil
}

// 加载监听文件
func loadWatchExportFile(errMode bool) {
	FileList, err := os.ReadDir(cfg.ExportDir)
	if err != nil {
		if errMode {
			panic("读取exportFile失败：" + err.Error())
		}
		return
	}
	for _, currFile := range FileList {
		filename := currFile.Name()
		if strings.HasSuffix(filename, ".ndjson") && isMatchPrefixs(filename, cfg.FilesPrefix) {
			fullFilePath := cfg.ExportDir + "/" + filename
			if !inArray(fullFilePath, listenFiles) {
				listenFiles = append(listenFiles, fullFilePath)
				go watch(fullFilePath)
			}
		}
	}
}

// 判断字符串是否在数组中
func inArray(value string, arr []string) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}

// 监听一个文件变动行
func watch(fileName string) {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: cfg.WatchWhence},
		MustExist: false,
		Poll:      false,
	}
	tails, err := tail.TailFile(fileName, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
		return
	}

	var (
		line      *tail.Line
		ok        bool
		jsonBytes []byte
		valueType int8
		itemID    int64
		clock     int64
		floatVal  float64
		int64Val  int64
		stringVal string
	)
	for {
		line, ok = <-tails.Lines
		if !ok {
			fmt.Printf("tail file close reopen, filename：%s\n", tails.Filename)
			time.Sleep(time.Second)
			continue
		}

		jsonBytes = []byte(line.Text)
		vType, err := jsonparser.GetInt(jsonBytes, "type")
		if err != nil {
			logChan <- "解析行记录`" + line.Text + "` type失败：" + err.Error() + "\n"
			atomic.AddUint64(&sendFailedCount, 1)
			continue
		}
		valueType = int8(vType)

		itemID, _ = jsonparser.GetInt(jsonBytes, "itemid")
		clock, _ = jsonparser.GetInt(jsonBytes, "clock")
		if itemID == 0 || clock == 0 {
			logChan <- "解析行记录`" + line.Text + "` itemid失败：" + err.Error() + "\n"
			atomic.AddUint64(&sendFailedCount, 1)
			continue
		}

		switch valueType {
		case 0:
			floatVal, _ = jsonparser.GetFloat(jsonBytes, "value")
			messageValueType0 <- &ItemRecordRow[float64]{
				itemid:     itemID,
				clock:      int(clock),
				value_type: valueType,
				value:      floatVal,
			}
		case 3:
			int64Val, _ = jsonparser.GetInt(jsonBytes, "value")
			messageValueType3 <- &ItemRecordRow[int64]{
				itemid:     itemID,
				clock:      int(clock),
				value_type: valueType,
				value:      int64Val,
			}
		case 1:
			stringVal, _ = jsonparser.GetString(jsonBytes, "value")
			stringVal = strings.ReplaceAll(stringVal, "'", "''")
			messageValueType1 <- &ItemRecordRow[string]{
				itemid:     itemID,
				clock:      int(clock),
				value_type: valueType,
				value:      stringVal,
			}
		case 4:
			stringVal, _ = jsonparser.GetString(jsonBytes, "value")
			stringVal = strings.ReplaceAll(stringVal, "'", "''")
			messageValueType4 <- &ItemRecordRow[string]{
				itemid:     itemID,
				clock:      int(clock),
				value_type: valueType,
				value:      stringVal,
			}
		case 2:
			stringVal, _ = jsonparser.GetString(jsonBytes, "value")
			stringVal = strings.ReplaceAll(stringVal, "'", "''")
			messageValueType2 <- &ItemRecordRow[string]{
				itemid:     itemID,
				clock:      int(clock),
				value_type: valueType,
				value:      stringVal,
			}
		}
	}
}

// 定时发送心跳信号
func pollerSenderHeartbeat() {
	heartBeatTicker := time.NewTicker(time.Second * 1) // 每秒心跳定时器

	var (
		startTimeClock int64 = time.Now().Unix()
		currentClock   int64
	)

	for {
		<-heartBeatTicker.C
		currentClock = time.Now().Unix() - startTimeClock

		if currentClock%2 == 0 {
			messageValueType0 <- nil
			messageValueType1 <- nil
			messageValueType2 <- nil
			messageValueType3 <- nil
			messageValueType4 <- nil
		}

		if currentClock%5 == 0 { // 每隔5秒
			logChan <- HEART_BEAT
		}

		if currentClock%60 == 0 { // 每隔600秒
			loadWatchExportFile(false)

			if _,err := os.Stat(cfg.LogPath); err != nil {
				loggerFile.Close()
			}

			printRunInfo()
		}
	}

}

// 发送控制
// 1.消息数量达到发送窗口大小时批量写入数据库
// 2.超过最大等待时间未更新，若当前buffer区存在数据则及时写入数据库（避免无数据触发时延迟）
func sender[T int64 | float64 | string](messageChan chan *ItemRecordRow[T], valueType int8, sendWindowSize int, sendWindowTimeout int64) {
	var (
		sendBufferIndex = 0
		sendRecords     []*ItemRecordRow[T]
		recordsBuffer         = make([]*ItemRecordRow[T], 0)
		lastSendTime    int64 = 0
	)

	for {
		itemRecord := <-messageChan

		if itemRecord == nil {
			// 距离上一次更新超过最大等待时间，若存在数据立即写入数据库
			if sendBufferIndex > 0 && time.Now().Unix()-lastSendTime >= sendWindowTimeout {
				sendRecords = make([]*ItemRecordRow[T], sendBufferIndex)
				copy(sendRecords, recordsBuffer)
				sendBufferIndex = 0 // 下一次循环从0开始填充
				go sendToDatabase(&sendRecords, valueType)
				lastSendTime = time.Now().Unix()
			}
			continue
		}

		acceptTotalCount++

		if len(recordsBuffer)-1 < sendBufferIndex {
			recordsBuffer = append(recordsBuffer, itemRecord)
		} else {
			recordsBuffer[sendBufferIndex] = itemRecord
		}
		sendBufferIndex++

		// 控制发送窗口
		if sendBufferIndex >= sendWindowSize {
			sendRecords = make([]*ItemRecordRow[T], sendWindowSize)
			copy(sendRecords, recordsBuffer)
			sendBufferIndex = 0
			go sendToDatabase(&sendRecords, valueType)
			lastSendTime = time.Now().Unix()
		}
	}
}

// 判断字符串是否匹配一组前缀
func isMatchPrefixs(str string, prefixs []string) bool {
	for _, prefix := range prefixs {
		if strings.HasPrefix(str, prefix) {
			return true
		}
	}
	return false
}

// 写入数据到数据库
func sendToDatabase[T int64 | float64 | string](data *[]*ItemRecordRow[T], valueType int8) {
	var (
		sqlExpression    string
		valuesExpression string
		valueField       string
	)

	valueField = valueFieldsMap[valueType]

	if cfg.DBType == "mysql" {
		sqlExpression = "REPLACE INTO lw_history_latest(itemid,clock,value_type," + valueField + ") values {{ValuesExpression}}"
	} else if cfg.DBType == "postgres" {
		sqlExpression = "INSERT INTO lw_history_latest(itemid,clock,value_type," + valueField + ") values {{ValuesExpression}} ON CONFLICT (itemid) DO UPDATE SET " + valueField + " = EXCLUDED." + valueField + ", clock = EXCLUDED.clock"
	} else {
		return
	}

	valueItemStr := "(%d,%d,%d,%v)," // 数值字符
	if valueType == 1 || valueType == 2 || valueType == 4 {
		valueItemStr = "(%d,%d,%d,'%v')," // 字符类型
	}

	dataRowsValueExpressMap := map[int64]string{}
	for _, rowItem := range *data {
		_,exist := dataRowsValueExpressMap[rowItem.itemid]
		if exist { // 当前批存在相同itemid（此处乐观地认为zabbix实时导出是按时间顺序输出的记录）
			atomic.AddUint64(&skipRefreshCount, 1)
		}
		dataRowsValueExpressMap[rowItem.itemid] = fmt.Sprintf(valueItemStr, rowItem.itemid, rowItem.clock, rowItem.value_type, rowItem.value)
	}
	data = nil
	for _,rowExpression := range dataRowsValueExpressMap {
		valuesExpression += rowExpression
	}
	recordRowCount := uint64(len(dataRowsValueExpressMap))
	dataRowsValueExpressMap = nil

	if recordRowCount <= 0 {
		return
	}

	valuesExpression = strings.TrimRight(valuesExpression, ",")
	sqlExpression = strings.ReplaceAll(sqlExpression, "{{ValuesExpression}}", valuesExpression)
	result, err := dbConn.Exec(sqlExpression)
	if err != nil {
		logChan <- fmt.Sprintf("执行sql语句 `%v` 发生错误：%v \n", sqlExpression, err)
		atomic.AddUint64(&sendFailedCount, recordRowCount)
		return
	}

	affectNum, err := result.RowsAffected()
	if err != nil {
		logChan <- fmt.Sprintf("更新value_type为%v获取受影响行发生错误：%v \n", valueType, err)
		atomic.AddUint64(&sendFailedCount, recordRowCount)
		return
	}

	atomic.AddUint64(&sendSuccessCount, recordRowCount) // uint64类型非协程安全，并发读写需要使用原子操作，逻辑等价于 sendSuccessCount++
	logChan <- fmt.Sprintf("更新%v条value_type为%v的记录，受影响行：%v \n", recordRowCount, valueType, affectNum)
}

// 加载数据库
func loadDatabase(errMode bool) *sql.DB {
	dbType := cfg.DBType
	username := cfg.Username
	password := cfg.Password
	host := cfg.Host
	port := cfg.Port
	databaseName := cfg.DatabaseName

	var dsn string
	if dbType == "mysql" {
		dsn = fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", username, password, host, port, databaseName)
	} else {
		dsn = fmt.Sprintf("host=%v port=%v dbname=%v user=%v password=%v sslmode=disable", host, port, databaseName, username, password)
	}

	conn, err := sql.Open(dbType, dsn)

	conn.SetMaxOpenConns(cfg.DBMaxOpenConn) // 最大连接数
	conn.SetMaxIdleConns(2)                 // 空闲时维持的连接池数

	if err != nil {
		if errMode {
			panic("数据库连接异常：" + err.Error())
		}
		fmt.Println("数据库连接异常：", err)
		return nil
	}
	err = conn.Ping()
	if err != nil {
		if errMode {
			panic("数据库连接ping异常：" + err.Error())
		}
		fmt.Println("数据库连接ping异常：", err)
	}

	dbConn = conn
	return dbConn
}

// 检查最新记录表，不存在则创建
func checkHistoryTableExist() {
	tableName := cfg.TablePrefix + "history_latest"
	r, err := dbConn.Query(fmt.Sprintf("select * from %s limit 1", tableName))
	if err == nil {
		r.Close()
		return
	}

	var createTableSql string
	if cfg.DBType == "mysql" {
		createTableSql = `
		CREATE TABLE %s (
			itemid bigint(20) unsigned NOT NULL,
			clock int(11) unsigned NOT NULL DEFAULT '0',
			value_type tinyint(3) unsigned NOT NULL,
			value_float double NOT NULL DEFAULT '0',
			value_str varchar(255) NOT NULL DEFAULT '',
			value_text text,
			value_uint bigint(20) unsigned NOT NULL DEFAULT '0',
			value_log text,
			PRIMARY KEY (itemid) USING BTREE
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		`
	} else if cfg.DBType == "postgres" {
		createTableSql = `
		CREATE TABLE "%s" (
			"itemid" int8 NOT NULL,
			"clock" int4 NOT NULL,
			"value_type" int2 NOT NULL,
			"value_float" float8 NOT NULL DEFAULT 0,
			"value_str" varchar(255) NOT NULL DEFAULT '',
			"value_text" text,
			"value_uint" int8 NOT NULL DEFAULT 0,
			"value_log" text,
			PRIMARY KEY ("itemid")
		  );
		`
	}

	_, err = dbConn.Exec(fmt.Sprintf(createTableSql, tableName))
	if err != nil {
		panic(fmt.Sprintf("初始化最新记录表（%v）失败：%v", tableName, err.Error()))
	}

	fmt.Printf("创建最新记录表 %v 成功\n", tableName)
}

// 定时写入磁盘 | 达到阈值写入磁盘
func registerLogger() {
	var logBuffer string = ""
	var lastWriteClock int64 = 0

	for {
		message := <-logChan
		if message == HEART_BEAT {
			if logBuffer != "" &&  time.Now().Unix() - lastWriteClock >= 5{
				WriteToLoggerFile(&logBuffer)
				lastWriteClock = time.Now().Unix()
				logBuffer = ""
			}
			continue
		}

		message = fmt.Sprintf("%s %s", time.Now().Format("2006-01-02 15:04:05"), message)
		logBuffer += message
		if len(logBuffer) > 4096000 {
			WriteToLoggerFile(&logBuffer)
			lastWriteClock = time.Now().Unix()
			logBuffer = ""
		}
	}

}

// 写入到日志文件
func WriteToLoggerFile(content *string) {
	_, err := loggerFile.WriteString(*content)
	if err != nil {
		fmt.Println("写入日志文件错误：" + err.Error())
		err = initLoggerFile()
		if err == nil {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05") + " 重新载入日志文件：" + cfg.LogPath)
			_, err = loggerFile.WriteString(*content)
			if err != nil {
				fmt.Println("尝试第二次写入日志文件错误：" + err.Error())
			}
		} else {
			fmt.Printf("%v 载入日志文件（%v）发生错误：%v \n", time.Now().Format("2006-01-02 15:04:05"), cfg.LogPath, err)
		}
	}
}

func printRunInfo() {
	fmt.Println("Listen Files：", listenFiles)
	fmt.Println("Accept Total Count：", acceptTotalCount)
	fmt.Println("Refresh Success Count：", sendSuccessCount)
	fmt.Println("Refresh Failed Count：", sendFailedCount)
	fmt.Println("Refresh Skip Count：", skipRefreshCount)
}
