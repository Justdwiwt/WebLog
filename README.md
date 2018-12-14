# 网站流量统计案例概述

## 背景说明

网站流量统计是改进网站服务的重要手段之一，通过获取用户在网站的行为，可以分析出哪些内容受到欢迎，哪些页面存在问题，从而使网站改进活动更具有针对性。

## 统计指标说明

常用的网站流量统计指标一般包括以下情况分析：

1. 按在线情况分析

    在线情况分析分别记录在线用户的活动信息，包括：来访时间、访客地域、来路页面、当前停留页面等，这些功能对企业实时掌握自身网站流量有很大的帮助。

2. 按时段分析

    时段分析提供网站任意时间内的流量变化情况.或者某一段时间到某一段时间的流量变化，比如小时段分布，日访问量分布，对于企业了解用户浏览网页的的时间段有一个很好的分析。

3. 按来源分析

    来源分析提供来路域名带来的来访次数、IP、独立访客、新访客、新访客浏览次数、站内总浏览次数等数据。这个数据可以直接让企业了解推广成效的来路，从而分析出那些网站投放的广告效果更明显。

**项目统计的指标说明**

| 指标    | 指标含义                                                                      |
| ------- | ----------------------------------------------------------------------------- |
| PV      | Page View 页面访问量。                                                        |
| &nbsp;  | 两种方式来统计:                                                               |
| &nbsp;  | 1.离线批处理统计，统计一天之内总的pv                                          |
| &nbsp;  | 2.实时分析                                                                    |
| &nbsp;  | 用户访问一次网站任意一页面，就算作一次PV,包括刷新也算                         |
| UV      | 总的独立访客数，按人头来计算，即统计有多少不同的用户数                        |
| &nbsp;  | 实现思路：每个用户初次访问网站时，会为此用户分配一个全局唯一的用户id, uvid。 |
| &nbsp;  | 然后放到用户浏览起的cookie里。以后用户再次访问时，都会携带此uvid             |
| &nbsp;  | 总的独立会话数。                                                              |
| &nbsp;  | 1.当关闭浏览器，再次打开，算作一个新会话                                      |
| &nbsp;  | 2.当一个会话超过30分钟未操作，再次操作，也算作一个新会话                      |
| &nbsp;  | 实现思路：每当产生一个新会话，就会为其分配一个全局唯一的会话id （SSID）       |
| BR      | 页面跳出率=跳出会话数/总的独立会话数                                          |
| &nbsp;  | 页面跳出率是衡量网站优良性的标准。跳出率越高，说明网站对于用户的粘性越低      |
| NewCust | 新增用户数，用uvid去历史数据做比对                                            |
| &nbsp;  | 如果此uvid从未出现过，就记为此用户是一个新增用户数                            |
| NewIp   | 新增Ip数，思路同上，指标换成ip地址                                            |
| AvgDeep | 平均的会话访问深度=总的会话访问深度/总的独立会话数                            |
| &nbsp;  | 一个会话的访问深度：浏览过哪些不同的url地址                                   |
| &nbsp;  | 举例：                                                                        |
| &nbsp;  | 会话1  http://a.jsp   http://b.jsp  http://a.jsp                              |
| &nbsp;  | 会话2  http://b.jsp                                                           |
| &nbsp;  | (2+1)/2=1.5                                                                   |
| AvgTime | 平均的会话访问时长=总的会话访问时长/总的独立会话数                            |
| &nbsp;  | 举例：                                                                        |
| &nbsp;  | 会话1      访问a.jsp   时间戳1                                                |
| &nbsp;  | 访问b.jsp  时间戳2                                                            |
| &nbsp;  | 访问c.jsp   时间戳3                                                           |
| &nbsp;  | 会话2      a.jsp  4分钟                                                       |
| &nbsp;  | b.jsp  6分钟                                                                  |
| &nbsp;  | 一般情况下，计算出时长的理论值要小于真实值，                                  |
| &nbsp;  | 因为最后一个页面的时长获取不到                                                |
