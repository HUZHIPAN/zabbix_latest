# 最新记录表维护
  基于zabbix的实时导出协议，封装最新记录表维护，提升历史记录表检索性能

### 最新记录表	

​			表前缀_history_latest

## 表结构

```sql
CREATE TABLE `lw_history_latest` (
  `itemid` bigint(20) unsigned NOT NULL,
  `clock` int(11) unsigned NOT NULL DEFAULT '0',
  `value_type` tinyint(3) unsigned NOT NULL,
  `value_float` double NOT NULL DEFAULT '0',
  `value_str` varchar(255) NOT NULL DEFAULT '',
  `value_text` text,
  `value_uint` bigint(20) unsigned NOT NULL DEFAULT '0',
  `value_log` text,
  PRIMARY KEY (`itemid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```





### 实现描述

​	根据zabbix_server实时导出协议，监听每一行产生的监控项json数据 ，实时推送到最新记录表，支持pg、mysql，最新记录表不存在自动创建
