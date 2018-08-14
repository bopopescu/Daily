/**
 * Created by Henry on 16/12/7.
 */
var mongo = require("mongodb");

var config = require("../common/config");
var async = require('async');
var log4js = require('log4js');
var logger = log4js.getLogger("summary");
// { liupan add, 2017/9/23
var fs = require('fs');
// } liupan add, 2017/9/23
var request = require('request');

log4js.configure({
    appenders: [
        {
            type: 'file', //文件输出
            // { liupan modify, 2017/9/7
            // filename: '/usr/local/iCStat/log_displayer/logs/summary.log',
            filename: config.log_path + "summary.log",
            // } liupan modify, 2017/9/7
            maxLogSize: 1024,
            backups:3,
            category: 'info'
        }
    ]
});
logger.setLevel('ALL');

//var ips = require("../common/ips");

function connect_mongo(res,callback){
    var MongoClient = require('mongodb').MongoClient;
    MongoClient.connect(config.mongo_addr, function (err, db) {
            if(!err){
                callback(db)
            }else{
                logger.error(err)
                res.json({
                    ErrNo:"100",
                    ErrMsg:"Failed to connect database"
                })
            }
    })
}

var last_five = function(req,res){
    try{
        connect_mongo(res,function(db){
            db.collection('log_table',function(err,tb){
                if(!err){
                    var five_ago = new Date(new Date().getTime()-600000);
                    var start = parseInt(
                        new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                        parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                    )
                    //console.log(start)

                    var query = {
                        'start':start
                    }
                    var back = {
                        start:1,
                        s_ip:1,
                        band:1,
                        suc_r:1,
                        freeze_r:1,
                        bitrate:1,
                        _id:0
                    }
                    tb.find(query,back).toArray(function(err,logs){
                        if(!err){
                            res.json(logs)
                            db.close()
                        }else{
                            res.json({
                                ErrNo:"102",
                                ErrMsg:"Failed to get logs"
                            })
                        }
                    })
                }else{
                    res.json({
                        ErrNo:"101",
                        ErrMsg:"Failed to get table"
                    })
                }
            })
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}



var get_time = function(req,res){
    try{
        connect_mongo(res,function(db){
            db.collection('log_table',function(err,tb){
                if(!err){
                    //var start = parseInt(req.query.time)
                    var time = req.query.time || "201701010000"
                    var start = parseInt(new Date(
                            parseInt(time.substring(0,4)),
                            parseInt(time.substring(4,6))-1,
                            parseInt(time.substring(6,8)),
                            parseInt(time.substring(8,10)),
                            parseInt(time.substring(10,12)),
                            0
                        ).getTime()/1000-3600*8)
                    var query = {
                        'start':start
                    }
                    req.query.sip && (query.s_ip = req.query.sip)
                    var back = {
                        from:1,
                        start:1,
                        s_ip:1,
                        band:1,
                        suc_r:1,
                        freeze_r:1,
                        bitrate:1,
                        _id:0
                    }
                    tb.find(query,back).toArray(function(err,logs){
                        if(!err){
                            res.json(logs)
                            db.close()
                        }else{
                            res.json({
                                ErrNo:"102",
                                ErrMsg:"Failed to get logs"
                            })
                        }
                    })
                }else{
                    res.json({
                        ErrNo:"101",
                        ErrMsg:"Failed to get table"
                    })
                }
            })
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}


var complete = function(req,res){
    ips.get(function(ip_obj){
        try{
            connect_mongo(res,function(db){
                db.collection('log_table',function(err,tb){
                    if(!err){
                        var five_ago = new Date(new Date().getTime()-300000);
                        var start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                                parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                        //console.log(start)

                        var query = {
                            'start':start
                        }
                        var back = {
                            s_ip:1,
                            _id:0
                        }
                        tb.find(query,back).toArray(function(err,logs){
                            if(!err){
                                var kw_ips = ip_obj.kw;
                                var dl_ips = ip_obj.dl;
                                var ws_ips = ip_obj.ws;
                                var kw_num = 0;
                                var dl_num = 0;
                                var ws_num = 0;
                                var current;
                                var ips = ""
                                for(var l in logs){
                                    logs[l].s_ip && (ips += (logs[l].s_ip+","))
                                }
                                var kw_list = ""
                                for(var i in kw_ips){
                                    current = kw_ips[i];
                                    if(ips.indexOf(current)>=0){
                                        kw_list += "<p style='color: green'>"+current+"</p>"
                                        kw_num ++;
                                    }else{
                                        kw_list += "<p style='color: red'>"+current+"</p>"
                                    }
                                }
                                kw_list = "<p>[KW total:"+kw_num+"/"+kw_ips.length+"]</p>" + kw_list;
                                var dl_list = ""
                                for(var j in dl_ips){
                                    current = dl_ips[j];
                                    if(ips.indexOf(current)>=0){
                                        dl_list += "<p style='color: green'>"+current+"</p>"
                                        dl_num ++;
                                    }else{
                                        dl_list += "<p style='color: red'>"+current+"</p>"
                                    }
                                }
                                dl_list = "<p>[DL total:"+dl_num+"/"+dl_ips.length+"]</p>" + dl_list;
                                var ws_list = ""
                                for(var k in ws_ips){
                                    current = ws_ips[k];
                                    if(ips.indexOf(current)>=0){
                                        ws_list += "<p style='color: green'>"+current+"</p>"
                                        ws_num ++;
                                    }else{
                                        ws_list += "<p style='color: red'>"+current+"</p>"
                                    }
                                }
                                ws_list = "<p>[WS total:"+ws_num+"/"+ws_ips.length+"]</p>" + ws_list;
                                res.send(kw_list+"<p></p>"+dl_list+"<p></p>"+ws_list)
                                db.close()
                            }else{
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }else{
                        res.json({
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            })
        }catch(e){
            res.json({
                ErrNo:"100",
                ErrMsg:"数据库错误"
            })
        }
    })
}
var test = function(req,res){
    ips.get(function(ips){
        res.send(ips)
    })
}
var cdn_band = function(req,res){
    try{
        connect_mongo(res,function(db){
            db.collection('log_table',function(err,tb){
                if(!err){
                    var start;
                    if(req.query.time){
                        var req_time = req.query.time
                        start = parseInt(new Date(
                                parseInt(req_time.substring(0,4)),
                                parseInt(req_time.substring(4,6))-1,
                                parseInt(req_time.substring(6,8)),
                                parseInt(req_time.substring(8,10)),
                                parseInt(req_time.substring(10,12)),
                                0
                            ).getTime()/1000-3600*8)
                    }else{
                        var five_ago = new Date(new Date().getTime()-300000);
                        start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                                parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                    }

                    var query = {
                        'start':start
                    }
                    var back = {
                        from:1,
                        start:1,
                        band:1,
                        _id:0
                    }
                    var band_collection = {
                        "kw":0,
                        "dl":0,
                        "ws":0,
                        "kwn":0,
                        "dln":0,
                        "wsn":0
                    }
                    tb.find(query,back).toArray(function(err,logs){
                        if(!err){
                            for(var l in logs){
                                var log = logs[l];
                                if(log.from&&log.band){
                                    switch(log.from){
                                        case 1:
                                            band_collection.kw += parseInt(log.band);
                                            band_collection.kwn += 1;
                                            break;
                                        case 2:
                                            band_collection.dl += parseInt(log.band);
                                            band_collection.dln += 1;
                                            break;
                                        case 3:
                                            band_collection.ws += parseInt(log.band);
                                            band_collection.wsn += 1;
                                            break;
                                    }
                                }
                            }
                            var time = new Date((3600*8+start)*1000)
                            band_collection.time = time.getHours()+":"+time.getMinutes()
                            res.json(band_collection)
                            db.close()
                        }else{   
                            db.close()
                            res.json({
                                ErrNo:"102",
                                ErrMsg:"Failed to get logs"
                            })
                        }
                    })
                }else{
                    db.close()
                    res.json({
                        ErrNo:"101",
                        ErrMsg:"Failed to get table"
                    })
                }
            })
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

var day_max = function(req,res){
    try{
        connect_mongo(res,function(db){
            db.collection('log_table',function(err,tb){
                if(!err){


                    var start;
                    if(req.query.time){
                        var req_time = req.query.time
                        start = parseInt(new Date(
                                parseInt(req_time.substring(0,4)),
                                parseInt(req_time.substring(4,6))-1,
                                parseInt(req_time.substring(6,8)),
                                parseInt(req_time.substring(8,10)),
                                parseInt(req_time.substring(10,12)),
                                0
                            ).getTime()/1000-3600*8)
                    }else{
                        var five_ago = new Date(new Date().getTime()-300000);
                        start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                                parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                    }

                    var query = {
                        'start':start
                    }
                    var back = {
                        from:1,
                        start:1,
                        band:1,
                        _id:0
                    }
                    var band_collection = {
                        "kw":0,
                        "dl":0,
                        "ws":0,
                        "kwn":0,
                        "dln":0,
                        "wsn":0
                    }
                    tb.find(query,back).toArray(function(err,logs){
                        if(!err){
                            for(var l in logs){
                                var log = logs[l];
                                if(log.from&&log.band){
                                    switch(log.from){
                                        case 1:
                                            band_collection.kw += parseInt(log.band);
                                            band_collection.kwn += 1;
                                            break;
                                        case 2:
                                            band_collection.dl += parseInt(log.band);
                                            band_collection.dln += 1;
                                            break;
                                        case 3:
                                            band_collection.ws += parseInt(log.band);
                                            band_collection.wsn += 1;
                                            break;
                                    }
                                }
                            }
                            var time = new Date((3600*8+start)*1000)
                            band_collection.time = time.getHours()+":"+time.getMinutes()
                            res.json(band_collection)
                            db.close()
                        }else{
                            res.json({
                                ErrNo:"102",
                                ErrMsg:"Failed to get logs"
                            })
                        }
                    })
                }else{
                    res.json({
                        ErrNo:"101",
                        ErrMsg:"Failed to get table"
                    })
                }
            })
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
// { liupan modify, 2017/8/1
// // { liupan modify, 2017/7/11
// if (config.node_version == 'JAPAN') {
//     var region_detail = ["jp"];
//     var operator_detail = ["sdzx"];
//     var cdn_detail=["sdzx"];
//     var operator_locate={"sdzx":1};
// }
// else {
//     var region_detail=["北京","上海","广东","天津","安徽","重庆","福建","甘肃","广西","贵州","海南","河北","河南","黑龙江","湖北","湖南","吉林","江苏","江西","辽宁","内蒙古","宁夏","青海","山东","山西","陕西","四川","台湾","西藏","香港","澳门","新疆","云南","浙江"];
//     var operator_detail=["联通","电信","移动","其他"];
//     var cdn_detail=["kw","ws","dl"];
//     var operator_locate={"联通":1,"电信":2,"移动":3,"其他":4}
// }
// // } liupan modify, 2017/7/11
var region_detail = config.regions.split(",");
var operator_detail = config.operators.split(",");
operator_detail[operator_detail.length] = "其他";
var operator_locate = {};
for (var i = 0; i < operator_detail.length; ++i) {
    operator_locate[operator_detail[i]] = i + 1;
}
var cdn_detail = [];
var cdn_detail_temp = config.cdns.split(",");
for (var i = 0; i < cdn_detail_temp.length; ++i) {
    var temp = cdn_detail_temp[i].split(":");
    cdn_detail[i] = temp[0];
}
var bitrate_type_list = {};
var bitrate_type_list_temp = config.bitrate_type.split(",");
for (var i = 0; i < bitrate_type_list_temp.length; ++i) {
    var temp = bitrate_type_list_temp[i].split(":");
    bitrate_type_list[temp[0]] = temp[1];
}
// } liupan modify, 2017/8/1
var node_time_stamp=1488457500;
var user_time_stamp=1489409220;
var node_history_start_time=1488457500;
var node_history_end_time=1488457500;

//获取当前数据的接口函数：运行数据
var statistics_interface_func=function(req,res)
{
    //console.log(req.body);
    time_stamp=new Date();
    node_time_stamp=parseInt(new Date(time_stamp.getFullYear(),time_stamp.getMonth(),
        time_stamp.getDate(),time_stamp.getHours(),time_stamp.getMinutes(),0
        )/1000)-300;
    logger.info("node_time:"+node_time_stamp);
    //console.log('statistics_interface_func')
    //var min=time_stamp.getMinutes()-time_stamp.getMinutes()%3
    // user_time_stamp=parseInt(new Date(time_stamp.getFullYear(),time_stamp.getMonth(),
    //     time_stamp.getDate(),time_stamp.getHours(),min,0
    //     )/1000)-540;
    user_time_stamp=parseInt(new Date(time_stamp.getFullYear(),time_stamp.getMonth(),
        time_stamp.getDate(),time_stamp.getHours(),time_stamp.getMinutes(),0
        )/1000)-300;
    //node_time_stamp=1493949600;
    logger.info("user_time:"+user_time_stamp);
    //base:node为基于节点统计。user为基于用户统计
    if(req.body.base == "node")
    {
        if(req.body.region == "全部")
        {
            //地图
            logger.info('all regions');
            get_all_statistics_func(req,res);
            return;
        }
        else if ( req.body.cdn == "none" && req.body.operator == "none" && req.body.special == 0)
        {
            //地图点击某地区 即第二层
            logger.info('single region');
            get_region_statistics_func(req,res);
            return;
        }
        else if ( req.body.cdn != "none" && req.body.operator != "none" && req.body.special == 0)
        {
            //地图点击某地区后点击详情  即第三层
            logger.info('single region ip');
            get_detail_statistics_func(req,res);
            return;
        }
        else if (req.body.special==1)
        {
            //重点区域监控   右边列表。
            logger.info("Enter Special region stat");
            get_special_region_statistics_func(req,res);
            return;
        }
    }
    else if(req.body.base == "user")
    {
        if(req.body.region == "全部")
        {
            //地图
            logger.info('all user regions');
            get_all_user_statistics_func2(req,res);
            return;
        }
        else if ( req.body.cdn == "none" && req.body.operator == "none" && req.body.special == 0)
        {
            //地图点击某地区 即第二层
            logger.info('single user region');
            get_region_user_statistics_func(req,res);
            return;
        }
        else if ( req.body.cdn != "none" && req.body.operator != "none" && req.body.special == 0)
        {
            //地图点击某地区后点击详情 即第三层
            logger.info('single user region ip');
            get_detail_user_statistics_func(req,res);
            return;
        }
        else if (req.body.special==1)
        {
            //重点区域监控   右边列表。
            logger.info("Enter user Special region stat");
            get_special_region_user_statistics_func(req,res);
            return;
        }
    }
}

var TIME_INTERVAL_MAX=172800;
var LIMIT_COUNT=1;

//获取历史数据的接口函数:带宽统计、历史数据。
var history_interface_func=function(req,res)
{
    console.log(req.body);
    node_history_start_time = Number(req.body.start);
    node_history_end_time = Number(req.body.end);
    logger.info(node_history_start_time);
    logger.info(node_history_end_time);
    LIMIT_COUNT=parseInt((node_history_end_time-node_history_start_time)/TIME_INTERVAL_MAX)+3;
    logger.info('LIMIT_COUNT='+LIMIT_COUNT);
    //base：node为基于节点的统计。user为基于用户的统计。
    if(req.body.base == "node")
    {
        region_temp=req.body.region;
        operator_temp=req.body.operator;
        cdn_temp=req.body.cdn;
        logger.info(region_temp+operator_temp+cdn_temp)
        if(region_temp==0&&operator_temp==0&&cdn_temp==0)
        {
            var allcdn = req.body.allcdn;
            if(allcdn == 1)
            {
                //基于节点的全网统计：带宽统计。
                // { liupan modify, 2017/8/1
                // logger.info('get_node_sum_cdn_history_func');
                // if(config.node_version=='CNTV')
                // {
                //     //CNTV拓扑
                //     get_node_sum_cdn_history_func(req,res);
                // }
                // else if(config.node_version=='LD_KW')
                // {
                //     //LD_KW拓扑。
                //     get_node_sum_history_func(req,res);
                // }
                // // { liupan add, 2017/7/11
                // else if(config.node_version == 'JAPAN')
                // {
                //     //JAPAN拓扑。
                //     // { liupan modify, 2017/7/20
                //     //get_node_sum_history_func(req,res);
                //     get_node_sum_jp_history_func(req,res);
                //     // } liupan modify, 2017/7/20
                // }
                // // } liupan add, 2017/7/11
                logger.info('get_node_sum_cdn_history_common_func');
                get_node_sum_cdn_history_common_func(req, res);
                // } liupan modify, 2017/8/1
            }
            else
            {
                //基于节点的全网统计：历史数据中勾选全网的情况。
                logger.info('get_node_sum_history_func');
                get_node_sum_history_func(req,res);
            }
        }
        // { liupan add, 2018/6/13
        // else if (region_temp == 0 && operator_temp == 0 && cdn_temp != 0) {
        //     //某个CDN的码率、卡顿、带宽、成功率的统计。
        //     logger.info('get_node_cdn_info_func');
        //     get_node_cdn_info_func(req, res);
        // }
        // } liupan add, 2018/6/13
        else
        {
            //基于节点的分步统计。
            logger.info('get_node_history_func');
            get_node_history_func(req,res);
        }
        
    }
    else if(req.body.base == "user")
    {
        var region_temp=req.body.region;
        var operator_temp=req.body.operator;
        var cdn_temp=req.body.cdn;
        var channel_temp = req.body.channel;
        var node_ip = req.body.node_ip;
        var his_type=req.body.history_type;
        if(region_temp==0&&operator_temp==0&&cdn_temp==0&&channel_temp==0 &&node_ip==0)
        {
            var allcdn = req.body.allcdn;
            if(allcdn == 1)
            {
                //基于用户的全网统计：带宽统计。
                // { liupan modify, 2017/8/1
                // logger.info('get_user_sum_cdn_history_func');
                // if(config.node_version=='CNTV')
                // {
                //     //CNTV拓扑
                //     get_user_sum_cdn_history_func(req,res);
                // }
                // else if(config.node_version=='LD_KW')
                // {
                //     //LD_KW拓扑。
                //     get_user_sum_history_func(req,res);
                // }
                // // { liupan add, 2017/7/11
                // else if(config.node_version == 'JAPAN')
                // {
                //     //JAPAN拓扑。
                //     get_user_sum_history_func(req, res);
                // }
                // // } liupan add, 2017/7/11
                logger.info('get_user_sum_cdn_history_common_func');
                get_user_sum_cdn_history_common_func(req, res);
                // } liupan modify, 2017/8/1
            }
            else
            {
                //基于用户的全网统计：历史数据中勾选全网的情况。
                logger.info('get_user_sum_history_func');
                get_user_sum_history_func(req,res);
            }
        }
        else if (channel_temp!=0 &&his_type=='rate_percentage'&&node_ip==0)
        {
            //频道码率占比的统计。
            // { liupan modify, 2017/8/1
            // logger.info('get_channel_percent_func');
            // if(config.node_version=='CNTV')
            // {
            //     //CNTV拓扑
            //     get_CNTV_channel_percent_func2(req,res);
            // }
            // else if(config.node_version=='LD_KW')
            // {
            //     //LD_KW拓扑。
            //     get_ld_kw_channel_percent_func(req,res);
            // }
            // // { liupan add, 2017/7/11
            // else if(config.node_version == 'JAPAN')
            // {
            //     //JAPAN拓扑。
            //     get_ld_kw_channel_percent_func(req, res);
            // }
            // // } liupan add, 2017/7/11
            logger.info('get_channel_percent_common_func');
            get_channel_percent_common_func(req, res);
            // } liupan modify, 2017/8/1
        }
        else if (channel_temp!=0&&node_ip==0)
        {
            //频道 码率、卡顿、带宽、成功率的统计。
            logger.info('get_channel_cdn_func');
            get_channel_cdn_func(req,res);
        }
        // { liupan add, 2018/6/11
        else if (region_temp == 0 && operator_temp == 0 && cdn_temp != 0 && node_ip==0) {
            //某个CDN的码率、卡顿、带宽、成功率的统计。
            logger.info('get_user_cdn_info_func');
            get_user_cdn_info_func(req,res);
        }
        // } xuzj add, 2018/8/13
        else if (node_ip != 0){
            logger.info("get_user_by_node_func");
            get_user_by_node_func(req, res);
        }
        // } liupan add, 2018/6/11
        else
        {
             //基于用户的分步统计。
            logger.info('get_user_history_func');
            get_user_history_func(req,res);
        }
    }
}

// { liupan add, 2018/6/13
// var get_node_cdn_info_func = function(req, res) {
//     try{
//         connect_mongo(res, function(db) {
//                 db.collection('history_node_sum', function(err, tb) {
//                     if (!err) {
//                         var query = {
//                             "time": {
//                                 "$gt":node_history_start_time,
//                                 "$lt":node_history_end_time
//                             }
//                         }

//                         var cdn_tmp = req.body.cdn;
//                         var historyType = req.body.history_type;

//                         var back = {
//                             "_id":0,
//                             "time":1,
//                             "cdn":1
//                         }

//                         var result = {"detail": {}};
//                         var max_min_value_dic = {};
//                         if (historyType == "band_width") {
//                             max_min_value_dic = {'max':{'all':{'band_width':-999999},'sy':{'band_width':0}},'min':{'all':{'band_width':9999999999},'sy':{'band_width':0}}};
//                         }
//                         else {
//                             max_min_value_dic = {'max':-999999,'min':9999999999};
//                         }
//                         var max_min_time_dic = {'max':0,'min':0};

//                         tb.find(query, back).toArray(function(err, logs) {
//                             if(!err) {
//                                 if (logs.length == 0) {
//                                     res.json('err len(logs)=0');
//                                     db.close();
//                                     return;
//                                 }

//                                 var output = {};
//                                 var out_count = {};
//                                 for (var i = 0; i < logs.length; i++) {
//                                     if (!logs[i]['cdn']) {
//                                         continue;
//                                     }

//                                     var data_temp = logs[i]['cdn'];
//                                     if (!(cdn_tmp in data_temp)) {
//                                         continue;
//                                     }
//                                     if (!(type in data_temp[cdn_tmp])) continue;

//                                     var time_temp = logs[i]['time'];

//                                     if (historyType == "band_width") {
//                                         if (!(time_temp in output)) {
//                                             output[time_temp] = {"all":{"band_width":0}, "sy":{"band_width":0}};
//                                         }
//                                         output[time_temp]["all"]["band_width"] = logs[i]['cdn'][cdn_tmp]["band_width"];
//                                         output[time_temp]["sy"]["band_width"] = logs[i]['cdn'][cdn_tmp]["sy_band_width"];
//                                     }
//                                     else {
//                                         output[time_temp] = logs[i]['cdn'][cdn_tmp][historyType];
//                                     }
//                                     out_count[time_temp] = 1;
//                                 }

//                                 var band_arr = [];
//                                 var value_count = 0;
//                                 var value_sum = 0;
//                                 var value_first_time = 0;
//                                 var out_result = {};
//                                 var test_count = 0;
//                                 var ori_count = 0;
//                                 var sy_value_sum = 0;
//                                 var all_output = {};
//                                 if (historyType == 'band_width') {
//                                     for (var t in output) {
//                                         band_arr.push(output[t]["all"]["band_width"]);
//                                         var data_time_stamp = t;
//                                         var data_value = output[t]["all"]["band_width"];
//                                         if (data_value > max_min_value_dic['max']["all"]["band_width"]) {
//                                             max_min_value_dic['max'] = output[t];
//                                             max_min_time_dic['max'] = data_time_stamp;
//                                         }
//                                         if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
//                                             max_min_value_dic['min'] = output[t];
//                                             max_min_time_dic['min'] = data_time_stamp;
//                                         }

//                                         ori_count += 1;
//                                         if (value_count == 0) {
//                                             value_first_time = t;
//                                         }
//                                         value_count += 1;
//                                         value_sum += output[t]["all"]["band_width"];
//                                         sy_value_sum += output[t]["sy"]["band_width"];
//                                         if (value_count >= LIMIT_COUNT) {
//                                             out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};

//                                             out_result[value_first_time]['all']['band_width'] = value_sum / LIMIT_COUNT;
//                                             out_result[value_first_time]['sy']['band_width'] = sy_value_sum / LIMIT_COUNT;
//                                             test_count += 1;
//                                             value_sum = 0;
//                                             sy_value_sum = 0;
//                                             value_count = 0;
//                                         }
//                                     }
//                                 }
//                                 else
//                                 {
//                                     for(var t in output)
//                                     {
//                                         if(out_count[t] != 0) {
//                                             if (historyType != 'user_n' && historyType != 'req_n')
//                                                 output[t]=output[t]/out_count[t];
//                                         }
//                                         var data_time_stamp = t;
//                                         var data_value = output[t];
//                                         if (data_value > max_min_value_dic['max']) {
//                                             max_min_value_dic['max']=data_value;
//                                             max_min_time_dic['max']=data_time_stamp;
//                                         }
//                                         if (data_value < max_min_value_dic['min']) {
//                                             max_min_value_dic['min'] = data_value;
//                                             max_min_time_dic['min'] = data_time_stamp;
//                                         }

//                                         ori_count += 1;
//                                         if (value_count == 0) {
//                                             value_first_time = t;
//                                         }
//                                         value_count += 1;
//                                         value_sum += output[t];
//                                         if(value_count >= LIMIT_COUNT) {
//                                             out_result[value_first_time] = value_sum / LIMIT_COUNT;
//                                             test_count += 1;
//                                             value_sum = 0;
//                                             value_count = 0;
//                                         }
//                                     }
//                                 }

//                                 out_result[max_min_time_dic['max']] = max_min_value_dic['max'];
//                                 out_result[max_min_time_dic['min']] = max_min_value_dic['min'];
//                                 test_out = [];
//                                 switch(historyType) {
//                                     case "band_width":
//                                         var len = parseInt(band_arr.length*0.95) - 1;
//                                         var sort_result = quickSort(band_arr);
//                                         var band95 = sort_result[len];
//                                         all_output["detail"] = out_result;
//                                         all_output["band95"] = band95;
//                                         break;

//                                     case "bitrate":
//                                     case "freeze_rate":
//                                     case "success_rate":
//                                     case "user_n":
//                                     case "req_n":
//                                         all_output["detail"]=out_result;
//                                         break;

//                                     default:
//                                         break;
//                                 }

//                                 db.close();
//                                 res.json(all_output);
//                                 return;
//                             }
//                             else
//                             {
//                                 db.close();
//                                 res.json({
//                                     ErrNo:"102",
//                                     ErrMsg:"Failed to get logs"
//                                 });
//                             }
//                         })
//                     }
//                     else
//                     {
//                         db.close();
//                         res.json({
//                             Err:err,
//                             ErrNo:"101",
//                             ErrMsg:"Failed to get table"
//                         });
//                     }
//                 })
            
//         })
//     }catch(e){
//         res.json({
//             ErrNo:"100",
//             ErrMsg:"数据库错误"
//         });
//     }
// }
// } liupan add, 2018/6/13

// { liupan add, 2018/6/11
var get_user_cdn_info_func = function(req, res) {
    try{
        connect_mongo(res,function(db){
                db.collection('history_user_sum',function(err,tb){
                    if(!err)
                    {
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }

                        var cdn_tmp = req.body.cdn;
                        var historyType = req.body.history_type;

                        var back = {
                            "_id":0,
                            "time":1,
                            "cdn":1
                        }

                        var result = {"detail": {}};
                        var max_min_value_dic = {};
                        if (historyType == "band_width") {
                            max_min_value_dic={'max':{'all':{'band_width':-999999},'sy':{'band_width':0}},'min':{'all':{'band_width':9999999999},'sy':{'band_width':0}}};
                        }
                        // { liupan add, 2018/6/19
                        else if (historyType == "ps_freeze_rate") {
                            max_min_value_dic={'max':{'all':{'freeze_rate':-999999},'ps':{'freeze_rate':0}},'min':{'all':{'freeze_rate':9999999999},'ps':{'freeze_rate':0}}};
                        }
                        // } liupan add, 2018/6/19
                        else {
                            max_min_value_dic={'max':-999999,'min':9999999999};
                        }
                        var max_min_time_dic={'max':0,'min':0};

                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 ) {   
                                    res.json('err len(logs)=0');
                                    db.close();
                                    return;
                                }

                                var output = {};
                                var out_count = {};
                                var sum_jam = 0;
                                var sum_duration = 0;
                                for (var i=0; i <logs.length;i++) {
                                    if(!logs[i]['cdn']) {
                                        continue;
                                    }

                                    var data_temp=logs[i]['cdn'];
                                    if (!(cdn_tmp in data_temp)) {
                                        continue;
                                    }
                                    // { liupan add, 2018/6/12
                                    // if (!(type in data_temp[cdn_tmp])) continue;
                                    // } liupan add, 2018/6/12

                                    var time_temp=logs[i]['time'];

                                    if (historyType == "band_width") {
                                        if (!(time_temp in output)) {
                                            output[time_temp] = {"all":{"band_width":0}, "sy":{"band_width":0}};
                                        }
                                        output[time_temp]["all"]["band_width"] = logs[i]['cdn'][cdn_tmp]["band_width"];
                                        output[time_temp]["sy"]["band_width"] = logs[i]['cdn'][cdn_tmp]["sy_band_width"];
                                    }
                                    // { liupan add, 2018/6/19
                                    else if (historyType == "ps_freeze_rate") {
                                        if (!(time_temp in output)) {
                                            output[time_temp] = {"all":{"freeze_rate":0}, "ps":{"freeze_rate":0}};
                                        }
                                        output[time_temp]["all"]["freeze_rate"] = logs[i]['cdn'][cdn_tmp]["freeze_rate"];
                                        if ("ps_freeze_rate" in logs[i]['cdn'][cdn_tmp])
                                            output[time_temp]["ps"]["freeze_rate"] = logs[i]['cdn'][cdn_tmp]["ps_freeze_rate"];
                                    }

                                    else if (historyType == "jamnumperminute") {
                                         var jam_all = logs[i]['cdn'][cdn_tmp]["jam_all"];
                                         var duration = logs[i]['cdn'][cdn_tmp]["duration"];
                                         sum_jam += jam_all;
                                         sum_duration += duration;
                                         if (duration==0){
                                             data_value = 0}
                                             else {
                                             data_value = jam_all / (duration /1000 /60);
                                             };
                                         if (!(time_temp in output)) {
                                            output[time_temp] = data_value
                                        }else {
                                             output[time_temp] += data_value
                                         }
                                    }

                                    // } liupan add, 2018/6/19
                                    else {
                                        output[time_temp] = logs[i]['cdn'][cdn_tmp][historyType];
                                    }
                                    out_count[time_temp] = 1;
                                }

                                var band_arr=[];
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                var sy_value_sum=0;
                                var all_output={};
                                if(historyType=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        band_arr.push(output[t]["all"]["band_width"]);
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["band_width"];
                                        if (data_value > max_min_value_dic['max']["all"]["band_width"]) {
                                            max_min_value_dic['max'] = output[t];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                            max_min_value_dic['min'] = output[t];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["band_width"];
                                        sy_value_sum += output[t]["sy"]["band_width"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};

                                            out_result[value_first_time]['all']['band_width'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['sy']['band_width'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                    }
                                }
                                // { liupan add, 2018/6/19
                                else if (historyType == 'ps_freeze_rate')
                                {
                                    for(var t in output)
                                    {
                                        if (out_count[t] != 0) {
                                            output[t] = output[t] / out_count[t];
                                        }
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["freeze_rate"];
                                        if (data_value > max_min_value_dic['max']["all"]["freeze_rate"]) {
                                            max_min_value_dic['max'] = output[t];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["freeze_rate"]) {
                                            max_min_value_dic['min'] = output[t];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["freeze_rate"];
                                        sy_value_sum += output[t]["ps"]["freeze_rate"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};

                                            out_result[value_first_time]['all']['freeze_rate'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / LIMIT_COUNT;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                    }
                                }
                                // } liupan add, 2018/6/19
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                        {
                                            // { liupan add, 2018/6/12
                                            if (historyType != 'user_n' && historyType != 'req_n') 
                                            // } liupan add, 2018/6/12
                                            output[t]=output[t]/out_count[t];
                                        }
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        ori_count+=1;
                                        if(value_count==0)
                                        {
                                            value_first_time=t;
                                        }
                                        value_count+=1;
                                        value_sum+=output[t];
                                        if(value_count >=LIMIT_COUNT)
                                        {
                                            out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                            test_count+=1;
                                            value_sum=0;
                                            value_count=0;
                                        }
                                    }
                                }

                                // { liupan modify, 2018/6/19
                                // out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                // out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                for (var xxx in out_result) {
                                    out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                    out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                    break;
                                }
                                // } liupan modify, 2018/6/19
                                test_out=[];
                                switch(historyType)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        all_output["detail"]=out_result;
                                        all_output["bandwidth95"]=band95;
                                        break;
                                    case "jamnumperminute":
                                        all_output["detail"]=out_result;
                                        if(sum_duration==0){
                                            all_output["jamnumAverage"] = 0
                                        }else{
                                            all_output["jamnumAverage"] = sum_jam / (sum_duration / 1000 / 60);
                                        }
                                    // { liupan modify, 2018/6/12
                                    // case "bitrate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "freeze_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "success_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "user_n":
                                    case "req_n":
                                    // { liupan add, 2018/6/19
                                    case "ps_freeze_rate":
                                    // } liupan add, 2018/6/19
                                    case "freeze_avg_iv":
                                    case "delayed_avg":
                                        all_output["detail"]=out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/12
                                }
                                res.json(all_output);

                                db.close();
                                return;
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
// } liupan add, 2018/6/11

// { liupan add, 2017/9/23
/**
 * [readLocalIPs description]
 * @param  {[type]}   filename [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
var readLocalIPs = function(filename, callback) {
    fs.readFile(filename, function(err, data) {
        if (err) {
            callback(err, null);
            return;
        }

        // { liupan modify, 2017/11/29
        // var result = JSON.parse(data);
        var result = {};
        if (data != "") result = JSON.parse(data);
        // } liupan modify, 2017/11/29
        callback(null, result);
    });
}

/**
 * [writeLocalIPs description]
 * @param  {[type]}   filename [description]
 * @param  {[type]}   data     [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
var writeLocalIPs = function(filename, data, callback) {
    fs.writeFile(filename, data, function(err) {
        if (err) {
            callback(err, null);
            return;
        }

        callback(null, null);
    });
}

/**
 * [getCurrentAllNodes description]
 * @param  {[type]} req [description]
 * @param  {[type]} res [description]
 * @return {[type]}     [description]
 */
var getCurrentNodes = function(req, res) {
    var time_stamp=new Date();
    node_time_stamp=parseInt(new Date(time_stamp.getFullYear(),time_stamp.getMonth(),
        time_stamp.getDate(),time_stamp.getHours(),time_stamp.getMinutes(),0
        )/1000)-300;

    var req_region = req.body.region;
    if (req_region == null) req_region = "";

    try {
        connect_mongo(res, function(db){
            db.collection('statistic_node', function(err, tb) {
                if (err) {
                    db.close()
                    res.json({
                        Err:err,
                        ErrNo:"101",
                        ErrMsg:"Failed to get table"
                    });
                    return;
                }

                var query = {
                    'time':node_time_stamp
                }
                var back = {
                    _id:0
                }

                // { liupan add, 2018/3/8
                var cdn_list = [];
                if (req.body.cdn != null && req.body.cdn != undefined && req.body.cdn != 'none') {
                    cdn_list = req.body.cdn.split(",");
                    //query['node_statistics.cdn'] = {$in: cdn_list};
                }
                // } liupan add, 2018/3/8

                tb.find(query, back).toArray(function(err2, logs) {
                    if (err2) {
                        db.close();
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get colection"
                        });
                        return;
                    }

                    if (logs.length == 0 || !('node_statistics' in logs[0])) {
                        db.close();
                        res.json('数据为空');
                        return;
                    }

                    var data = logs[0].node_statistics;
                    var curNodes = {};
                    for (var i = 0; i < data.length; ++i) {
                        // { liupan add, 2018/3/8
                        var flag = false;
                        for (var x = 0; x < cdn_list.length; ++x) {
                            if (cdn_list[x] == data[i].cdn) {
                                flag = true;
                                break;
                            }
                        }
                        if (cdn_list.length == 0) flag = true;

                        if (!flag) continue;
                        // } liupan add, 2018/3/8
                        
                        var tempnode = data[i]['node'];
                        for (var j = 0; j < tempnode.length; ++j) {
                            if (req_region != "" && tempnode[j]['region'].indexOf(req_region) < 0) {
                                continue;
                            }

                            curNodes[tempnode[j]['ip']] = {'cdn':data[i].cdn, 'region':tempnode[j]['region'], 'operator':tempnode[j]['operator'], 'addtime':"", 'status':2};
                        }
                    }

                    var filename = [];
                    filename[0] = config.localIPs_file;
                    async.map(filename, function(item, callback) {
                        readLocalIPs(item, callback);
                    }, function(err, result) {
                        var localIPs = result[0];
                        if (err != null && err[0] != null) {
                            db.close();
                            res.json(curNodes);
                            return;
                        }

                        for (var ip in localIPs) {
                            if (req_region != "" && localIPs[ip]['region'].indexOf(req_region) < 0) {
                                continue;
                            }
                            // { liupan add, 2018/3/8
                            var flag = false;
                            for (var i = 0; i < cdn_list.length; ++i) {
                                if (cdn_list[i] == localIPs[ip]['cdn']) {
                                    flag = true;
                                    break;
                                }
                            }

                            if (cdn_list.length == 0) flag = true;
                            if (!flag) continue;
                            // } liupan add, 2018/3/8

                            if (curNodes.hasOwnProperty(ip)) {
                                curNodes[ip]['addtime'] = localIPs[ip]['addtime'];
                                curNodes[ip]['status'] = 1;
                            }
                            else {
                                curNodes[ip] = {'cdn':localIPs[ip]['cdn'], 'region':localIPs[ip]['region'], 'operator':localIPs[ip]['operator'], 'addtime':localIPs[ip]['addtime'], 'status':0};
                            }
                        }

                        db.close();
                        res.json(curNodes);
                    });
                });
            });
        });
    } catch(e) {
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        });
    }
}

/**
 * [addIP2Local description]
 * @param {[type]} req [description]
 * @param {[type]} res [description]
 */
var addIP2Local = function(req, res) {
    var filename = [];
    filename[0] = config.localIPs_file;
    async.map(filename, function(item, callback) {
        readLocalIPs(item, callback);
    }, function(err, result) {
        var localIPs = result[0];
        if (err != null && err[0] != null) {
            res.json("读取文件失败");
            return;
        }

        if (localIPs == null) {
            localIPs = {};
        }
        localIPs[req.body.ip] = {'cdn':req.body.cdn, 'region':req.body.region, 'operator':req.body.operator, 'addtime':Math.round(new Date().getTime()/1000), 'status':1};

        async.map(filename, function(item2, callback2) {
            writeLocalIPs(item2, JSON.stringify(localIPs), callback2);
        }, function(err2, result2) {
            if (err2 != null && err2[0] != null) {
                res.json("写文件失败");
                return;
            }

            res.json("OK");
        });
    });
}

/**
 * [deleteIPFromLocal description]
 * @param  {[type]} req [description]
 * @param  {[type]} res [description]
 * @return {[type]}     [description]
 */
var deleteIPFromLocal = function(req, res) {
    var filename = [];
    filename[0] = config.localIPs_file;
    async.map(filename, function(item, callback) {
        readLocalIPs(item, callback);
    }, function(err, result) {
        var localIPs = result[0];
        if (err != null && err[0] != null) {
            res.json("读取文件失败");
            return;
        }

        if (localIPs == null) {
            localIPs = {};
        }

        var newLocalIPs = {};
        for (var ip in localIPs) {
            if (ip == req.body.ip) continue;

            newLocalIPs[ip] = localIPs[ip];
        }

        async.map(filename, function(item2, callback2) {
            writeLocalIPs(item2, JSON.stringify(newLocalIPs), callback2);
        }, function(err2, result2) {
            if (err2 != null && err2[0] != null) {
                res.json("写文件失败");
                return;
            }

            res.json("OK");
        });
    });
}
// } liupan add, 2017/9/23

// { liupan add, 2017/9/26
var readIPList = function(data, cdn, callback) {
    var result = {};
    var lines = data.split("\n");
    // { liupan add, 2017/12/22
    var edgelevel = config.edgelevel.split(",");
    // } liupan add, 2017/12/22
    for (var i = 0; i < lines.length; ++i) {
        var tempstr = lines[i].split(" ");
        if (tempstr[0] == "") continue;

        // { liupan modify, 2017/12/22
        // if (Number(tempstr[1]) != 5) continue;
        var flag = 0;
        for (var e = 0; e < edgelevel.length; ++e) {
            if (Number(tempstr[1]) == Number(edgelevel[e])) {
                flag = 1;
                break;
            }
        }

        if (flag == 0) continue;
        // } liupan modify, 2017/12/22

        tempstr[2] = tempstr[2].split("\"")[1];
        var region = tempstr[2];
        var operator = tempstr[2];
        for (var j = 0; j < region_detail.length; ++j) {
            if (region.indexOf(region_detail[j]) >= 0) {
                region = region_detail[j];
                break;
            }
        }

        for (var j = 0; j < operator_detail.length; ++j) {
            if (operator.indexOf(operator_detail[j]) >= 0) {
                operator = operator_detail[j];
                break;
            }
        }

        if (result.hasOwnProperty(tempstr[0]) == false) {
            result[tempstr[0]] = {'cdn':cdn, 'region':region, 'operator':operator};
        }
    }

    callback(null, result);
}

var downloadIPList = function(url, cdn, callback) {
    var filename = "/usr/local/iCStat/logdisplayer/config/iplist.ini";
    var http = require('http');
    http.get(url, function(res) {
        var pageData = "";
        res.on('data', function (chunk) {
            pageData += chunk;
        });

        res.on('end', function () {
            readIPList(pageData, cdn, callback);
        });

        res.on('error', function (errget) {
            callback(errget, null);
        });
    });
    /*var stream = fs.createWriteStream(filename);
    request(url).pipe(stream).on('close', function() {
        readIPList(filename, cdn, callback);
    });*/
}

var getCurrentNodes_IPList = function(req, res) {
    var time_stamp=new Date();
    node_time_stamp=parseInt(new Date(time_stamp.getFullYear(),time_stamp.getMonth(),
        time_stamp.getDate(),time_stamp.getHours(),time_stamp.getMinutes(),0)/1000)-300;

    var req_region = req.body.region;
    if (req_region == null) req_region = "";

    try {
        connect_mongo(res, function(db){
            db.collection('statistic_node', function(err, tb) {
                if (err) {
                    db.close()
                    res.json({
                        Err:err,
                        ErrNo:"101",
                        ErrMsg:"Failed to get table"
                    });
                    return;
                }

                var query = {
                    'time':node_time_stamp
                }
                var back = {
                    _id:0
                }

                // { liupan add, 2018/3/8
                var cdn_list = [];
                if (req.body.cdn != null && req.body.cdn != undefined && req.body.cdn != 'none') {
                    cdn_list = req.body.cdn.split(",");
                    //query['node_statistics.cdn'] = {$in: cdn_list};
                }
                // } liupan add, 2018/3/8

                tb.find(query, back).toArray(function(err2, logs) {
                    if (err2) {
                        db.close();
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get colection"
                        });
                        return;
                    }

                    if (logs.length == 0 || !('node_statistics' in logs[0])) {
                        db.close();
                        res.json('数据为空');
                        return;
                    }

                    var data = logs[0].node_statistics;
                    var curNodes = {};
                    for (var i = 0; i < data.length; ++i) {
                        // { liupan add, 2018/3/8
                        var flag = false;
                        for (var x = 0; x < cdn_list.length; ++x) {
                            if (cdn_list[x] == data[i].cdn) {
                                flag = true;
                                break;
                            }
                        }
                        if (cdn_list.length == 0) flag = true;

                        if (!flag) continue;
                        // } liupan add, 2018/3/8
                        
                        var tempnode = data[i]['node'];
                        for (var j = 0; j < tempnode.length; ++j) {
                            if (req_region != "" && tempnode[j]['region'].indexOf(req_region) < 0) {
                                continue;
                            }

                            curNodes[tempnode[j]['ip']] = {'cdn':data[i].cdn, 'region':tempnode[j]['region'], 'operator':tempnode[j]['operator'], 'status':2};
                        }
                    }

                    // { liupan modify, 2017/11/3
                    // var filename = [];
                    // filename[0] = "http://124.243.198.150:8000/cache/hudong.list";
                    // async.map(filename, function(item, callback) {
                    //     downloadIPList(item, "ld_kw", callback);
                    // }, function(err, result) {
                    //     var localIPs = result[0];
                    //     if (err != null && err[0] != null) {
                    //         db.close();
                    //         res.json(curNodes);
                    //         return;
                    //     }
                    // 
                    //     for (var ip in localIPs) {
                    //         if (req_region != "" && localIPs[ip]['region'].indexOf(req_region) < 0) {
                    //             continue;
                    //         }
                    // 
                    //         if (curNodes.hasOwnProperty(ip)) {
                    //             curNodes[ip]['status'] = 1;
                    //         }
                    //         else {
                    //             curNodes[ip] = {'cdn':localIPs[ip]['cdn'], 'region':localIPs[ip]['region'], 'operator':localIPs[ip]['operator'], 'status':0};
                    //         }
                    //     }
                    // 
                    //     db.close();
                    //     res.json(curNodes);
                    // });
                    var url_list = [];
                    // { liupan modify, 2017/11/28
                    // url_list[0] = {"url":"http://124.243.198.150:8000/cache/hudong.list", "name":"ld_kw"};
                    // url_list[1] = {"url":"http://115.238.170.246:8000/file/bsy.list", "name":"bsy"};
                    var index = 0;
                    for (var key in config.ip_list) {
                        // { liupan add, 2018/3/8
                        var flag = false;
                        for (var i = 0; i < cdn_list.length; ++i) {
                            if (cdn_list[i] == key) {
                                flag = true;
                                break;
                            }
                        }
                        if (cdn_list.length == 0) flag = true;

                        if (!flag) continue;
                        // } liupan add, 2018/3/8
                        url_list[index] = {"name": key, "url": config.ip_list[key]};
                        index++;
                    }
                    // } liupan modify, 2017/11/28
                    async.map(url_list, function(item, callback) {
                        downloadIPList(item["url"], item["name"], callback);
                    }, function(err, result) {
                        if (err != null) {
                            db.close();
                            res.json(curNodes);
                            return;
                        }

                        for (var i = 0; i < result.length; ++i) {
                            var localIPs = result[i];
                            for (var ip in localIPs) {
                                if (req_region != "" && localIPs[ip]['region'].indexOf(req_region) < 0) {
                                    continue;
                                }

                                if (curNodes.hasOwnProperty(ip)) {
                                    curNodes[ip]['status'] = 1;
                                }
                                else {
                                    curNodes[ip] = {'cdn':localIPs[ip]['cdn'], 'region':localIPs[ip]['region'], 'operator':localIPs[ip]['operator'], 'status':0};
                                }
                            }
                        }

                        db.close();
                        res.json(curNodes);
                    });
                    // } liupan modify, 2017/11/3
                });
            });
        });
    } catch(e) {
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        });
    }
}
// } liupan add, 2017/9/26

//获取所有的统计数据
var get_all_statistics_func = function(req,res){
    var output={};
    try{
        connect_mongo(res,function(db)
        {
                db.collection('statistic_node',function(err,tb)
                {
                    if(!err)
                    {
                        var query = {
                            'time':node_time_stamp
                        }
                        var back = {
                            _id:0
                        }
                        // { liupan add, 2018/3/8
                        var cdn_list = [];
                        if (req.body.cdn != "none") {
                            cdn_list = req.body.cdn.split(",");
                        }
                        // } liupan add, 2018/3/8
                        // { liupan add, 2018/3/21
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                        }
                        // } liupan add, 2018/3/21
                        // { liupan add, 2018/4/18
                        var region_list = [];
                        if (req.body.region_list != null && req.body.region_list != undefined && req.body.region_list != "none") {
                            region_list = req.body.region_list.split(",");
                        }
                        // } liupan add, 2018/4/18
                        logger.info("begin tb.find :"+node_time_stamp);
                        tb.find(query,back).toArray(function(err,logs)
                        //tb.aggregate([{ $unwind: "$node_statistics" }, { $match:query}]).toArray(function(err, logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 || !('node_statistics' in logs[0]))
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var data=logs[0].node_statistics;
                                var region_count={};
                                var all_output={};
                                var all_count={"sum":0,"center":0,'bit_time':0};
                                all_output["_bitrate"]=0;
                                all_output["_freeze_rate"]=0;
                                all_output["_band_width"]=0;
                                all_output["_success_rate"]=0;
                                for(var i =0 ;i<data.length;i++)
                                {
                                    // { liupan add, 2018/3/8
                                    var flag = false;
                                    for (var x = 0; x < cdn_list.length; ++x) {
                                        if (cdn_list[x] == data[i].cdn) {
                                            flag = true;
                                            break;
                                        }
                                    }
                                    if (cdn_list.length == 0) flag = true;

                                    // { liupan add, 2018/4/18
                                    if (flag) {
                                        flag = false;
                                        for (var x = 0; x < region_list.length; ++x) {
                                            for (var n = 0; n < data[i]['node'].length; ++n) {
                                                if (data[i]['node'][n]['region'].indexOf(region_list[x]) >= 0) {
                                                    flag = true;
                                                    x = region_list.length;
                                                    break;
                                                }
                                            }
                                        }
                                        if (region_list.length == 0) flag = true;
                                    }
                                    // } liupan add, 2018/4/18

                                    if (!flag) continue;
                                    // } liupan add, 2018/3/8
                                    
                                    for (var j = 0;j < region_detail.length; j++)
                                    {
                                        // { liupan delete, 2018/3/8
                                        // if(req.body.cdn != "none")
                                        // {
                                        //     if(req.body.cdn != data[i].cdn+'')
                                        //         continue;
                                        // }
                                        // } liupan delete, 2018/3/8
                                        var region_temp = data[i].node[0].region+'';
                                        var cdn_temp = data[i].cdn+'';
                                        var region_ret = region_temp.indexOf(region_detail[j]);
                                        var level_temp = data[i].level;
                                        var operator_temp = data[i].node[0].operator+'';
                                        var vip_temp = data[i].vip;
                                        if (region_ret!=-1)
                                        {
                                            if(data[i].bitrate==0 && data[i].freeze_rate==0 && data[i].band_width==0 && data[i].success_rate==0)
                                            {
                                                continue;
                                            }
                                            //all_output["_bitrate"]+=data[i].bitrate;
                                            all_output["_bitrate"]+=data[i].bit_sum;
                                            all_output["_freeze_rate"]+=data[i].freeze_rate;
                                            all_output["_band_width"]+=data[i].band_width;
                                            all_output["_success_rate"]+=data[i].success_rate;
                                            all_count["sum"]+=1;
                                            all_count['bit_time']+=data[i].bit_time;
                                            if ((level_temp == 3 || level_temp == 4) && vip_temp!=1)
                                            {
                                                all_count["center"]+=1;
                                            }
                                            if (output[region_detail[j]])
                                            {
                                                output[region_detail[j]]["all"]["_bitrate"]+=data[i].bit_sum;
                                                output[region_detail[j]]["all"]["_freeze_rate"]+=data[i].freeze_rate;
                                                output[region_detail[j]]["all"]["_band_width"]+=data[i].band_width;
                                                output[region_detail[j]]["all"]["_success_rate"]+=data[i].success_rate;
                                                region_count[region_detail[j]]["all"]["sum"]+=1;
                                                region_count[region_detail[j]]["all"]["bit_time"]+=data[i].bit_time;
                                                {
                                                    if(output[region_detail[j]][cdn_temp])
                                                    {
                                                        region_count[region_detail[j]][cdn_temp]["sum"][0]+=1;
                                                        region_count[region_detail[j]][cdn_temp]["sum"][operator_locate[operator_temp]]+=1;
                                                        region_count[region_detail[j]][cdn_temp]["bit_time"][0]+=data[i].bit_time;
                                                        region_count[region_detail[j]][cdn_temp]["bit_time"][operator_locate[operator_temp]]+=data[i].bit_time;
                                                        if ((level_temp == 3 || level_temp == 4) && vip_temp!=1)
                                                        {
                                                            region_count[region_detail[j]]["all"]["center"]+=1;
                                                            region_count[region_detail[j]][cdn_temp]["center"][operator_locate[operator_temp]]+=1;
                                                            region_count[region_detail[j]][cdn_temp]["center"][0]+=1;
                                                        }
                                                        if(output[region_detail[j]][cdn_temp]["_bitrate"][operator_locate[operator_temp]]==-1)
                                                        {
                                                            output[region_detail[j]][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                            output[region_detail[j]][cdn_temp]["_bitrate"][0]+=data[i].bit_sum;
                                                        }
                                                        else
                                                        {
                                                            output[region_detail[j]][cdn_temp]["_bitrate"][operator_locate[operator_temp]]+=data[i].bit_sum;
                                                            output[region_detail[j]][cdn_temp]["_bitrate"][0]+=data[i].bit_sum;
                                                        }
                                                        if (output[region_detail[j]][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]==-1)
                                                        {
                                                            output[region_detail[j]][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate
                                                            output[region_detail[j]][cdn_temp]["_freeze_rate"][0]+=data[i].freeze_rate
                                                        }
                                                        else
                                                        {
                                                            output[region_detail[j]][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]+=data[i].freeze_rate
                                                            output[region_detail[j]][cdn_temp]["_freeze_rate"][0]+=data[i].freeze_rate
                                                        }
                                                        if (output[region_detail[j]][cdn_temp]["_success_rate"][operator_locate[operator_temp]]==-1)
                                                        {
                                                            output[region_detail[j]][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                            output[region_detail[j]][cdn_temp]["_success_rate"][0]+=data[i].success_rate;
                                                        }
                                                        else
                                                        {
                                                            output[region_detail[j]][cdn_temp]["_success_rate"][operator_locate[operator_temp]]+=data[i].success_rate;
                                                            output[region_detail[j]][cdn_temp]["_success_rate"][0]+=data[i].success_rate;
                                                        }
                                                        output[region_detail[j]][cdn_temp]["_band_width"]+=data[i].band_width;
                                                    }
                                                    else
                                                    {
                                                        region_count[region_detail[j]][cdn_temp]={};
                                                        region_count[region_detail[j]][cdn_temp]["sum"]=[1,0,0,0,0];
                                                        region_count[region_detail[j]][cdn_temp]["sum"][operator_locate[operator_temp]]=1;
                                                        region_count[region_detail[j]][cdn_temp]['bit_time']=[data[i].bit_time,0,0,0,0];
                                                        region_count[region_detail[j]][cdn_temp]["bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                        region_count[region_detail[j]][cdn_temp]["center"]=[0,0,0,0,0];
                                                        if ((level_temp == 3 || level_temp == 4) && vip_temp!=1)
                                                        {
                                                            region_count[region_detail[j]]["all"]["center"]+=1;
                                                            region_count[region_detail[j]][cdn_temp]["center"][operator_locate[operator_temp]]=1;
                                                            region_count[region_detail[j]][cdn_temp]["center"][0]=1;
                                                        }
                                                        else
                                                        {
                                                            region_count[region_detail[j]]["all"]["center"]+=0;
                                                        }
                                                        output[region_detail[j]][cdn_temp]={};
                                                        output[region_detail[j]][cdn_temp]["_bitrate"]=[data[i].bit_sum,-1,-1,-1,-1];
                                                        output[region_detail[j]][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                        output[region_detail[j]][cdn_temp]["_freeze_rate"]=[data[i].freeze_rate,-1,-1,-1,-1];
                                                        output[region_detail[j]][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate;
                                                        output[region_detail[j]][cdn_temp]["_band_width"]=data[i].band_width;
                                                        output[region_detail[j]][cdn_temp]["_success_rate"]=[data[i].success_rate,-1,-1,-1,-1];
                                                        output[region_detail[j]][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                region_count[region_detail[j]]={};
                                                region_count[region_detail[j]]["all"]={"sum":1,'bit_time':data[i].bit_time};
                                                region_count[region_detail[j]][cdn_temp]={};
                                                region_count[region_detail[j]][cdn_temp]["center"]=[0,0,0,0,0];
                                                region_count[region_detail[j]][cdn_temp]["sum"]=[1,0,0,0,0];
                                                region_count[region_detail[j]][cdn_temp]["sum"][operator_locate[operator_temp]]=1;
                                                region_count[region_detail[j]][cdn_temp]['bit_time']=[data[i].bit_time,0,0,0,0];
                                                region_count[region_detail[j]][cdn_temp]["bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                if ((level_temp == 3 || level_temp == 4) && vip_temp!=1)
                                                {
                                                    region_count[region_detail[j]]["all"]["center"]=1;
                                                    region_count[region_detail[j]][cdn_temp]["center"][operator_locate[operator_temp]]=1;
                                                    region_count[region_detail[j]][cdn_temp]["center"][0]=1;
                                                }
                                                else
                                                {
                                                    region_count[region_detail[j]]["all"]["center"]=0;
                                                }
                                                output[region_detail[j]]={};
                                                output[region_detail[j]]["all"]={};
                                                output[region_detail[j]]["all"]["_bitrate"]=data[i].bit_sum;
                                                output[region_detail[j]]["all"]["_freeze_rate"]=data[i].freeze_rate;
                                                output[region_detail[j]]["all"]["_band_width"]=data[i].band_width;
                                                output[region_detail[j]]["all"]["_success_rate"]=data[i].success_rate;
                                                output[region_detail[j]][cdn_temp]={};
                                                output[region_detail[j]][cdn_temp]["_bitrate"]=[data[i].bit_sum,-1,-1,-1,-1];
                                                output[region_detail[j]][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                output[region_detail[j]][cdn_temp]["_freeze_rate"]=[data[i].freeze_rate,-1,-1,-1,-1];
                                                output[region_detail[j]][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate;
                                                output[region_detail[j]][cdn_temp]["_band_width"]=data[i].band_width;
                                                output[region_detail[j]][cdn_temp]["_success_rate"]=[data[i].success_rate,-1,-1,-1,-1];
                                                output[region_detail[j]][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                            }
                                            break;
                                        }
                                    }

                                }
                                if(all_output["_bitrate"]>0)
                                {
                                    all_output["_bitrate"]=(all_output["_bitrate"]/(all_count["bit_time"])).toFixed(2)
                                }
                                if(all_output["_freeze_rate"]>0)
                                {
                                    all_output["_freeze_rate"]=(all_output["_freeze_rate"]/(all_count["sum"]-all_count["center"])).toFixed(2)
                                }
                                if(all_output["_success_rate"]>0)
                                {
                                   all_output["_success_rate"]=(all_output["_success_rate"]/all_count["sum"]).toFixed(2)
                                }
                                all_output["_band_width"]=(all_output["_band_width"]-0).toFixed(2)

                                for(var region_out in output)
                                {
                                    for (var data_out in output[region_out])
                                    {
                                        if(data_out=="all")
                                        {
                                            if(output[region_out][data_out]._bitrate!=0)
                                            {
                                                output[region_out][data_out]._bitrate=(output[region_out][data_out]._bitrate/(region_count[region_out][data_out].bit_time)).toFixed(2)
                                            }
                                            if(output[region_out][data_out]._freeze_rate!=0)
                                            {
                                                output[region_out][data_out]._freeze_rate=(output[region_out][data_out]._freeze_rate/(region_count[region_out][data_out].sum-region_count[region_out][data_out].center)).toFixed(2)
                                            }
                                            if(output[region_out][data_out]._success_rate!=0)
                                            {
                                               output[region_out][data_out]._success_rate=(output[region_out][data_out]._success_rate/region_count[region_out][data_out].sum).toFixed(2)
                                            }
                                            output[region_out][data_out]._band_width=(output[region_out][data_out]._band_width-0).toFixed(2)
                                        }
                                        else
                                        {
                                            for(var i = 0; i < 6; i++)
                                            {
                                                if(output[region_out][data_out]["_bitrate"][i]>0)
                                                {
                                                    output[region_out][data_out]["_bitrate"][i]=(output[region_out][data_out]["_bitrate"][i]/(region_count[region_out][data_out]["bit_time"][i])).toFixed(2)
                                                }
                                                if(output[region_out][data_out]["_freeze_rate"][i]>0)
                                                {
                                                    output[region_out][data_out]["_freeze_rate"][i]=(output[region_out][data_out]["_freeze_rate"][i]/(region_count[region_out][data_out]["sum"][i]-region_count[region_out][data_out]["center"][i])).toFixed(2)
                                                }
                                                if(output[region_out][data_out]["_success_rate"][i]>0)
                                                {
                                                   output[region_out][data_out]["_success_rate"][i]=(output[region_out][data_out]["_success_rate"][i]/region_count[region_out][data_out]["sum"][i]).toFixed(2)
                                                }
                                                output[region_out][data_out]["_band_width"]=(output[region_out][data_out]["_band_width"]-0).toFixed(2)
                                            }
                                        }
                                    }
                                }


                                var all_out={};
                                all_out["node"]=output;
                                all_out["all"]=all_output;
                                //all_out.push(output);
                                //all_out.push(region_count);
                                //all_out.push(logs);
                                //res.json(output);
                                res.json(all_out);
                                db.close()
                                //console.log(output);
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

var get_all_user_statistics_func = function(req,res){
    var output={};
    var all_out=[];
    try{
        connect_mongo(res,function(db){
                db.collection('statistic_user',function(err,tb)
                {
                    if(!err)
                    {
                        var five_ago = new Date(new Date().getTime()-600000);
                        var start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                            parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                        //console.log(start)

                        var query = {
                            'time':user_time_stamp

                        }
                        var back = {
                            _id:0
                            ,"user_statistics.ip":0
                        }
                        
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                //logger.info("len="+logs.length);
                                //console.log(logs[0])
                                //res.json(logs);
                                //return;
                                if ( logs.length==0 || !('user_statistics' in logs[0]))
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }

                                
                                var region_count={};
                                var all_output={};
                                var all_count={"sum":0,"center":0};
                                all_output["_bitrate"]=0;
                                all_output["_freeze_rate"]=0;
                                all_output["_band_width"]=0;
                                all_output["_success_rate"]=0;
                                all_output["_bit_sum"]=0;
                                all_output["_bit_time"]=0;
                                // // res.json(output);
                                //console.log("data.length "+data.length);
                                for(var k=0;k<logs.length;k++)
                                {
                                    var data=logs[k].user_statistics;
                                    for(var i =0 ;i<data.length;i++)
                                    {
                                        if(data[i].bitrate==0 && data[i].freeze_rate==0 && data[i].band_width==0 && data[i].success_rate==0)
                                        {    
                                            continue;
                                        }
                                        if(req.body.cdn != "none")
                                        {
                                            if(req.body.cdn != data[i].cdn+'')
                                                continue;
                                        }
                                        all_output["_bitrate"]+=data[i].bitrate;
                                        all_output["_freeze_rate"]+=data[i].freeze_rate;
                                        all_output["_band_width"]+=data[i].band_width;
                                        all_output["_success_rate"]+=data[i].success_rate;
                                        all_output["_bit_sum"]+=data[i].bit_sum;
                                        all_output["_bit_time"]+=data[i].bit_time;
                                        //console.log("i = "+i);
                                        {
                                            var region_temp = data[i].region+'';
                                            var cdn_temp = data[i].cdn+'';
                                            //var region_ret = region_temp.indexOf(region_detail[j]);
                                            var level_temp = data[i].level;
                                            var operator_temp = data[i].operator+'';
                                            //console.log(operator_temp+level_temp+region_temp+cdn_temp);
                                            var vip_temp = data[i].vip;
                                            if (region_temp)
                                            {
                                                all_count["sum"]+=1;
                                                if ((level_temp == 3 || level_temp == 4) && vip_temp!=1)
                                                {
                                                    all_count["center"]+=1;
                                                }
                                                if (output[region_temp])
                                                {
                                                    output[region_temp]["all"]["_bitrate"]+=data[i].bitrate;
                                                    output[region_temp]["all"]["_freeze_rate"]+=data[i].freeze_rate;
                                                    output[region_temp]["all"]["_band_width"]+=data[i].band_width;
                                                    output[region_temp]["all"]["_success_rate"]+=data[i].success_rate;
                                                    output[region_temp]["all"]["_bit_sum"]+=data[i].bit_sum;
                                                    output[region_temp]["all"]["_bit_time"]+=data[i].bit_time;
                                                    region_count[region_temp]["all"]["sum"]+=1;

                                                    //if(cdn_temp)
                                                    {
                                                        if(output[region_temp][cdn_temp])
                                                        {
                                                            region_count[region_temp][cdn_temp]["sum"][0]+=1;
                                                            region_count[region_temp][cdn_temp]["sum"][operator_locate[operator_temp]]+=1;
                                                            if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                            {
                                                                region_count[region_temp]["all"]["center"]+=1;
                                                                region_count[region_temp][cdn_temp]["center"][operator_locate[operator_temp]]+=1;
                                                                region_count[region_temp][cdn_temp]["center"][0]+=1;
                                                            }
                                                            if(output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]==-1)
                                                            {
                                                                output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bitrate;
                                                                output[region_temp][cdn_temp]["_bitrate"][0]+=data[i].bitrate;
                                                                output[region_temp][cdn_temp]["_bit_sum"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                                output[region_temp][cdn_temp]["_bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                            }
                                                            else
                                                            {
                                                                output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]+=data[i].bitrate;
                                                                output[region_temp][cdn_temp]["_bitrate"][0]+=data[i].bitrate;
                                                                output[region_temp][cdn_temp]["_bit_sum"][operator_locate[operator_temp]]+=data[i].bit_sum;
                                                                output[region_temp][cdn_temp]["_bit_time"][operator_locate[operator_temp]]+=data[i].bit_time;
                                                            }
                                                            output[region_temp][cdn_temp]["_bit_sum"][0]+=data[i].bit_sum;
                                                            output[region_temp][cdn_temp]["_bit_time"][0]+=data[i].bit_time;

                                                            if (output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]==-1)
                                                            {
                                                                output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate
                                                                output[region_temp][cdn_temp]["_freeze_rate"][0]+=data[i].freeze_rate
                                                            }
                                                            else
                                                            {
                                                                output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]+=data[i].freeze_rate
                                                                output[region_temp][cdn_temp]["_freeze_rate"][0]+=data[i].freeze_rate
                                                            }

                                                            if (output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]==-1)
                                                            {
                                                                output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                                output[region_temp][cdn_temp]["_success_rate"][0]+=data[i].success_rate;
                                                            }
                                                            else
                                                            {
                                                                output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]+=data[i].success_rate;
                                                                output[region_temp][cdn_temp]["_success_rate"][0]+=data[i].success_rate;
                                                            }
                                                            output[region_temp][cdn_temp]["_band_width"]+=data[i].band_width;
                                                        }
                                                        else
                                                        {
                                                            region_count[region_temp][cdn_temp]={};
                                                            region_count[region_temp][cdn_temp]["sum"]=[1,0,0,0,0];
                                                            region_count[region_temp][cdn_temp]["sum"][operator_locate[operator_temp]]=1;
                                                            region_count[region_temp][cdn_temp]["center"]=[0,0,0,0,0];
                                                            if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                            {
                                                                region_count[region_temp]["all"]["center"]+=1;
                                                                region_count[region_temp][cdn_temp]["center"][operator_locate[operator_temp]]=1;
                                                                region_count[region_temp][cdn_temp]["center"][0]=1;
                                                            }
                                                            else
                                                            {
                                                                region_count[region_temp]["all"]["center"]+=0;
                                                            }
                                                            output[region_temp][cdn_temp]={};
                                                            output[region_temp][cdn_temp]["_bitrate"]=[data[i].bitrate,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bitrate;
                                                            output[region_temp][cdn_temp]["_bit_sum"]=[data[i].bit_sum,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_bit_sum"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                            output[region_temp][cdn_temp]["_bit_time"]=[data[i].bit_time,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                            output[region_temp][cdn_temp]["_freeze_rate"]=[data[i].freeze_rate,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate;
                                                            output[region_temp][cdn_temp]["_band_width"]=data[i].band_width;
                                                            output[region_temp][cdn_temp]["_success_rate"]=[data[i].success_rate,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    region_count[region_temp]={};
                                                    region_count[region_temp]["all"]={};
                                                    region_count[region_temp]["all"]["sum"]=1;
                                                    region_count[region_temp][cdn_temp]={};
                                                    region_count[region_temp][cdn_temp]["sum"]=[];
                                                    region_count[region_temp][cdn_temp]["center"]=[0,0,0,0,0];
                                                    region_count[region_temp][cdn_temp]["sum"]=[1,0,0,0,0];
                                                    region_count[region_temp][cdn_temp]["sum"][operator_locate[operator_temp]]=1;
                                                    if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                    {
                                                        region_count[region_temp]["all"]["center"]=1;
                                                        region_count[region_temp][cdn_temp]["center"][operator_locate[operator_temp]]=1;
                                                        region_count[region_temp][cdn_temp]["center"][0]=1;
                                                    }
                                                    else
                                                    {
                                                        region_count[region_temp]["all"]["center"]=0;
                                                    }
                                                    output[region_temp]={};
                                                    output[region_temp]["all"]={};
                                                    output[region_temp]["all"]["_bitrate"]=data[i].bitrate;
                                                    output[region_temp]["all"]["_bit_sum"]=data[i].bit_sum;
                                                    output[region_temp]["all"]["_bit_time"]=data[i].bit_time;
                                                    output[region_temp]["all"]["_freeze_rate"]=data[i].freeze_rate;
                                                    output[region_temp]["all"]["_band_width"]=data[i].band_width;
                                                    output[region_temp]["all"]["_success_rate"]=data[i].success_rate;
                                                    output[region_temp][cdn_temp]={};
                                                    output[region_temp][cdn_temp]["_bitrate"]=[data[i].bitrate,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bitrate;
                                                    output[region_temp][cdn_temp]["_bit_sum"]=[data[i].bit_sum,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_bit_sum"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                    output[region_temp][cdn_temp]["_bit_time"]=[data[i].bit_time,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                    output[region_temp][cdn_temp]["_freeze_rate"]=[data[i].freeze_rate,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate;
                                                    output[region_temp][cdn_temp]["_band_width"]=data[i].band_width;
                                                    output[region_temp][cdn_temp]["_success_rate"]=[data[i].success_rate,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                }
                                            }
                                        }
                                        
                                        //all_out.push(output);
                                    }
                                }

                                if(all_output["_bitrate"]>0)
                                {
                                    if(all_count["_bit_time"]!=null && all_count["_bit_time"]!=undefined && all_count["_bit_time"]!=0)
                                    {
                                        all_output["_bitrate"]=(all_output["_bit_sum"]/(all_count["_bit_time"])).toFixed(2);
                                        // console.log(all_output["_bit_sum"]);
                                        // console.log(all_output["_bit_time"]);
                                    }
                                    else
                                    {
                                        all_output["_bitrate"]=(all_output["_bitrate"]/(all_count["sum"]-all_count["center"])).toFixed(2);
                                    }
                                }
                                if(all_output["_freeze_rate"]>0)
                                {
                                    all_output["_freeze_rate"]=(all_output["_freeze_rate"]/(all_count["sum"]-all_count["center"])).toFixed(2)
                                }
                                if(all_output["_success_rate"]>0)
                                {
                                   all_output["_success_rate"]=(all_output["_success_rate"]/all_count["sum"]).toFixed(2)
                                }
                                all_output["_band_width"]=(all_output["_band_width"]-0).toFixed(2)


                                for(var region_out in output)
                                {
                                    for (var data_out in output[region_out])
                                    {
                                        if(data_out=="all")
                                        {
                                            var count_temp=(region_count[region_out][data_out].sum-region_count[region_out][data_out].center);
                                            if(output[region_out][data_out]._bitrate!=0 && count_temp!=0)
                                            {
                                                if(output[region_out][data_out]._bit_time!=null && output[region_out][data_out]._bit_time!=undefined && output[region_out][data_out]._bit_time!=0)
                                                {
                                                    output[region_out][data_out]._bitrate = (output[region_out][data_out]._bit_sum/output[region_out][data_out]._bit_time).toFixed(2);
                                                    // console.log(output[region_out][data_out]._bit_sum);
                                                    // console.log(output[region_out][data_out]._bit_time);
                                                }
                                                else
                                                {
                                                    output[region_out][data_out]._bitrate=(output[region_out][data_out]._bitrate/(region_count[region_out][data_out].sum-region_count[region_out][data_out].center)).toFixed(2)
                                                }
                                            }
                                            if(output[region_out][data_out]._freeze_rate!=0 && count_temp!=0 )
                                            {
                                                output[region_out][data_out]._freeze_rate=(output[region_out][data_out]._freeze_rate/(region_count[region_out][data_out].sum-region_count[region_out][data_out].center)).toFixed(2)
                                            }
                                            if(output[region_out][data_out]._success_rate!=0 )
                                            {
                                               output[region_out][data_out]._success_rate=(output[region_out][data_out]._success_rate/region_count[region_out][data_out].sum).toFixed(2)
                                            }
                                            output[region_out][data_out]._band_width=(output[region_out][data_out]._band_width-0).toFixed(2)
                                        }
                                        else
                                        {
                                            for(var i = 0; i < 6; i++)
                                            {
                                                var count_temp=(region_count[region_out][data_out]["sum"][i]-region_count[region_out][data_out]["center"][i]);
                                                if(output[region_out][data_out]["_bitrate"][i]>0 && count_temp!=0 )
                                                {
                                                    if(output[region_out][data_out]["_bit_time"][i]!=null && output[region_out][data_out]["_bit_time"][i]!=undefined && output[region_out][data_out]["_bit_time"][i]!=0)
                                                    {
                                                        output[region_out][data_out]["_bitrate"][i]=(output[region_out][data_out]["_bit_sum"][i]/output[region_out][data_out]["_bit_time"][i]).toFixed(2);
                                                        console.log(output[region_out][data_out]["_bit_sum"][i]);
                                                        console.log(output[region_out][data_out]["_bit_time"][i]);
                                                    }
                                                    else
                                                    {
                                                        output[region_out][data_out]["_bitrate"][i]=(output[region_out][data_out]["_bitrate"][i]/(region_count[region_out][data_out]["sum"][i]-region_count[region_out][data_out]["center"][i])).toFixed(2)
                                                    }
                                                }
                                                if(output[region_out][data_out]["_freeze_rate"][i]>0 && count_temp!=0 )
                                                {
                                                    output[region_out][data_out]["_freeze_rate"][i]=(output[region_out][data_out]["_freeze_rate"][i]/(region_count[region_out][data_out]["sum"][i]-region_count[region_out][data_out]["center"][i])).toFixed(2)
                                                }
                                                if(output[region_out][data_out]["_success_rate"][i]>0)
                                                {
                                                   output[region_out][data_out]["_success_rate"][i]=(output[region_out][data_out]["_success_rate"][i]/region_count[region_out][data_out]["sum"][i]).toFixed(2)
                                                }
                                                output[region_out][data_out]["_band_width"]=(output[region_out][data_out]["_band_width"]-0).toFixed(2)
                                            }
                                        }
                                    }
                                }
                                var all_out_put={};
                                all_out_put["node"]=output;
                                all_out_put["all"]=all_output;
                                res.json(all_out_put);
                            }
                            else
                            {
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    db.close();
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
var get_all_user_statistics_func2 = function(req,res){
    var output={};
    var all_out=[];
    try{
        connect_mongo(res,function(db){
                db.collection('statistic_user',function(err,tb)
                {
                    if(!err)
                    {
                        var five_ago = new Date(new Date().getTime()-600000);
                        var start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                            parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                        //console.log(start)

                        var query = {
                            'time':user_time_stamp

                        }
                        var back = {
                            _id:0
                            ,"user_statistics.ip":0
                        }
                        
                        // { liupan add, 2018/3/8
                        var cdn_list = [];
                        if (req.body.cdn != "none") {
                            cdn_list = req.body.cdn.split(",");
                        }
                        // } liupan add, 2018/3/8
                        // { liupan add, 2018/3/21
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                        }
                        // } liupan add, 2018/3/21
                        // { liupan add, 2018/4/18
                        var region_list = [];
                        if (req.body.region_list != null && req.body.region_list != undefined && req.body.region_list != "none") {
                            region_list = req.body.region_list.split(",");
                        }
                        // } liupan add, 2018/4/18

                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                //logger.info("len="+logs.length);
                                //console.log(logs[0])
                                //res.json(logs);
                                //return;
                                if ( logs.length==0 || !('user_statistics' in logs[0]))
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }

                                
                                var region_count={};
                                var all_output={};
                                var all_count={"sum":0,"center":0,'bit_time':0};
                                all_output["_bitrate"]=0;
                                all_output["_freeze_rate"]=0;
                                all_output["_band_width"]=0;
                                all_output["_success_rate"]=0;
                                // // res.json(output);
                                //console.log("data.length "+data.length);
                                for(var k=0;k<logs.length;k++)
                                {
                                    var data=logs[k].user_statistics;
                                    for(var i =0 ;i<data.length;i++)
                                    {
                                        // { liupan add, 2018/3/8
                                        var flag = false;
                                        for (var x = 0; x < cdn_list.length; ++x) {
                                            if (cdn_list[x] == data[i].cdn) {
                                                flag = true;
                                                break;
                                            }
                                        }
                                        if (cdn_list.length == 0) flag = true;

                                        // { liupan add, 2018/4/18
                                        if (flag) {
                                            flag = false;
                                            for (var x = 0; x < region_list.length; ++x) {
                                                if (data[i]['region'].indexOf(region_list[x]) >= 0) {
                                                    flag = true;
                                                    break;
                                                }
                                            }
                                            if (region_list.length == 0) flag = true;
                                        }
                                        // } liupan add, 2018/4/18

                                        if (!flag) continue;
                                        // } liupan add, 2018/3/8
                                        
                                        if(data[i].bitrate==0 && data[i].freeze_rate==0 && data[i].band_width==0 && data[i].success_rate==0)
                                        {    
                                            continue;
                                        }
                                        // { liupan delete, 2018/3/8
                                        // if(req.body.cdn != "none")
                                        // {
                                        //     if(req.body.cdn != data[i].cdn+'')
                                        //         continue;
                                        // }
                                        // } liupan delete, 2018/3/8
                                        if(req.body.cdn == "none" && data[i].cdn=='qt')
                                        {
                                            continue;
                                        }
                                        all_output["_bitrate"]+=data[i].bit_sum;
                                        all_output["_freeze_rate"]+=data[i].freeze_rate;
                                        all_output["_band_width"]+=data[i].band_width;
                                        all_output["_success_rate"]+=data[i].success_rate;
                                        all_count['bit_time']+=data[i].bit_time
                                        //console.log("i = "+i);
                                        {
                                            var region_temp = data[i].region+'';
                                            var cdn_temp = data[i].cdn+'';
                                            //var region_ret = region_temp.indexOf(region_detail[j]);
                                            var level_temp = data[i].level;
                                            var operator_temp = data[i].operator+'';
                                            //console.log(operator_temp+level_temp+region_temp+cdn_temp);
                                            var vip_temp = data[i].vip;
                                            if (region_temp)
                                            {
                                                
                                                all_count["sum"]+=1;
                                                if ((level_temp == 3 || level_temp == 4) && vip_temp!=1)
                                                {
                                                    all_count["center"]+=1;
                                                }
                                                if (output[region_temp])
                                                {
                                                    output[region_temp]["all"]["_bitrate"]+=data[i].bit_sum;
                                                    output[region_temp]["all"]["_freeze_rate"]+=data[i].freeze_rate;
                                                    output[region_temp]["all"]["_band_width"]+=data[i].band_width;
                                                    output[region_temp]["all"]["_success_rate"]+=data[i].success_rate;
                                                    region_count[region_temp]["all"]["sum"]+=1;
                                                    region_count[region_temp]["all"]["bit_time"]+=data[i].bit_time;
                                                    //if(cdn_temp)
                                                    {
                                                        if(output[region_temp][cdn_temp])
                                                        {
                                                            region_count[region_temp][cdn_temp]["sum"][0]+=1;
                                                            region_count[region_temp][cdn_temp]["sum"][operator_locate[operator_temp]]+=1;
                                                            region_count[region_temp][cdn_temp]["bit_time"][0]+=data[i].bit_time;
                                                            region_count[region_temp][cdn_temp]["bit_time"][operator_locate[operator_temp]]+=data[i].bit_time;
                                                            if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                            {
                                                                region_count[region_temp]["all"]["center"]+=1;
                                                                region_count[region_temp][cdn_temp]["center"][operator_locate[operator_temp]]+=1;
                                                                region_count[region_temp][cdn_temp]["center"][0]+=1;
                                                            }
                                                            if(output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]==-1)
                                                            {
                                                                output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                                output[region_temp][cdn_temp]["_bitrate"][0]+=data[i].bit_sum;
                                                            }
                                                            else
                                                            {
                                                                output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]+=data[i].bit_sum;
                                                                output[region_temp][cdn_temp]["_bitrate"][0]+=data[i].bit_sum;
                                                            }
                                                            if (output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]==-1)
                                                            {
                                                                output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate
                                                                output[region_temp][cdn_temp]["_freeze_rate"][0]+=data[i].freeze_rate
                                                            }
                                                            else
                                                            {
                                                                output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]+=data[i].freeze_rate
                                                                output[region_temp][cdn_temp]["_freeze_rate"][0]+=data[i].freeze_rate
                                                            }
                                                            if (output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]==-1)
                                                            {
                                                                output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                                output[region_temp][cdn_temp]["_success_rate"][0]+=data[i].success_rate;
                                                            }
                                                            else
                                                            {
                                                                output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]+=data[i].success_rate;
                                                                output[region_temp][cdn_temp]["_success_rate"][0]+=data[i].success_rate;
                                                            }
                                                            output[region_temp][cdn_temp]["_band_width"]+=data[i].band_width;
                                                        }
                                                        else
                                                        {
                                                            region_count[region_temp][cdn_temp]={};
                                                            region_count[region_temp][cdn_temp]["sum"]=[1,0,0,0,0];
                                                            region_count[region_temp][cdn_temp]["sum"][operator_locate[operator_temp]]=1;
                                                            region_count[region_temp][cdn_temp]['bit_time']=[data[i].bit_time,0,0,0,0];
                                                            region_count[region_temp][cdn_temp]["bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                            region_count[region_temp][cdn_temp]["center"]=[0,0,0,0,0];
                                                            if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                            {
                                                                region_count[region_temp]["all"]["center"]+=1;
                                                                region_count[region_temp][cdn_temp]["center"][operator_locate[operator_temp]]=1;
                                                                region_count[region_temp][cdn_temp]["center"][0]=1;
                                                            }
                                                            else
                                                            {
                                                                region_count[region_temp]["all"]["center"]+=0;
                                                            }
                                                            output[region_temp][cdn_temp]={};
                                                            output[region_temp][cdn_temp]["_bitrate"]=[data[i].bit_sum,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                            output[region_temp][cdn_temp]["_freeze_rate"]=[data[i].freeze_rate,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate;
                                                            output[region_temp][cdn_temp]["_band_width"]=data[i].band_width;
                                                            output[region_temp][cdn_temp]["_success_rate"]=[data[i].success_rate,-1,-1,-1,-1];
                                                            output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    region_count[region_temp]={};
                                                    region_count[region_temp]["all"]={"sum":1,'bit_time':data[i].bit_time};
                                                    region_count[region_temp][cdn_temp]={};
                                                    region_count[region_temp][cdn_temp]["center"]=[0,0,0,0,0];
                                                    region_count[region_temp][cdn_temp]["sum"]=[1,0,0,0,0];
                                                    region_count[region_temp][cdn_temp]["sum"][operator_locate[operator_temp]]=1;
                                                    region_count[region_temp][cdn_temp]['bit_time']=[data[i].bit_time,0,0,0,0];
                                                    region_count[region_temp][cdn_temp]["bit_time"][operator_locate[operator_temp]]=data[i].bit_time;
                                                    
                                                    if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                    {
                                                        region_count[region_temp]["all"]["center"]=1;
                                                        region_count[region_temp][cdn_temp]["center"][operator_locate[operator_temp]]=1;
                                                        region_count[region_temp][cdn_temp]["center"][0]=1;
                                                    }
                                                    else
                                                    {
                                                        region_count[region_temp]["all"]["center"]=0;
                                                    }
                                                    output[region_temp]={};
                                                    output[region_temp]["all"]={};
                                                    output[region_temp]["all"]["_bitrate"]=data[i].bit_sum;
                                                    output[region_temp]["all"]["_freeze_rate"]=data[i].freeze_rate;
                                                    output[region_temp]["all"]["_band_width"]=data[i].band_width;
                                                    output[region_temp]["all"]["_success_rate"]=data[i].success_rate;
                                                    output[region_temp][cdn_temp]={};
                                                    output[region_temp][cdn_temp]["_bitrate"]=[data[i].bit_sum,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_bitrate"][operator_locate[operator_temp]]=data[i].bit_sum;
                                                    output[region_temp][cdn_temp]["_freeze_rate"]=[data[i].freeze_rate,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]]=data[i].freeze_rate;
                                                    output[region_temp][cdn_temp]["_band_width"]=data[i].band_width;
                                                    output[region_temp][cdn_temp]["_success_rate"]=[data[i].success_rate,-1,-1,-1,-1];
                                                    output[region_temp][cdn_temp]["_success_rate"][operator_locate[operator_temp]]=data[i].success_rate;
                                                }
                                            }
                                        }
                                    }
                                }
                                console.log(all_output);
                                console.log(all_count);
                                if(all_output["_bitrate"]>0)
                                {
                                    all_output["_bitrate"]=(all_output["_bitrate"]/(all_count["bit_time"])).toFixed(2)
                                }
                                if(all_output["_freeze_rate"]>0)
                                {
                                    all_output["_freeze_rate"]=(all_output["_freeze_rate"]/(all_count["sum"]-all_count["center"])).toFixed(2)
                                }
                                if(all_output["_success_rate"]>0)
                                {
                                   all_output["_success_rate"]=(all_output["_success_rate"]/all_count["sum"]).toFixed(2)
                                }
                                all_output["_band_width"]=(all_output["_band_width"]-0).toFixed(2)


                                for(var region_out in output)
                                {
                                    for (var data_out in output[region_out])
                                    {
                                        if(data_out=="all")
                                        {
                                            var count_temp=(region_count[region_out][data_out].sum-region_count[region_out][data_out].center);
                                            if(output[region_out][data_out]._bitrate!=0 )
                                            {
                                                output[region_out][data_out]._bitrate=(output[region_out][data_out]._bitrate/(region_count[region_out][data_out].bit_time)).toFixed(2)
                                            }
                                            if(output[region_out][data_out]._freeze_rate!=0 && count_temp!=0 )
                                            {
                                                output[region_out][data_out]._freeze_rate=(output[region_out][data_out]._freeze_rate/(region_count[region_out][data_out].sum-region_count[region_out][data_out].center)).toFixed(2)
                                            }
                                            if(output[region_out][data_out]._success_rate!=0 )
                                            {
                                               output[region_out][data_out]._success_rate=(output[region_out][data_out]._success_rate/region_count[region_out][data_out].sum).toFixed(2)
                                            }
                                            output[region_out][data_out]._band_width=(output[region_out][data_out]._band_width-0).toFixed(2)
                                        }
                                        else
                                        {
                                            for(var i = 0; i < 6; i++)
                                            {
                                                var count_temp=(region_count[region_out][data_out]["sum"][i]-region_count[region_out][data_out]["center"][i]);
                                                if(output[region_out][data_out]["_bitrate"][i]>0 )
                                                {
                                                    output[region_out][data_out]["_bitrate"][i]=(output[region_out][data_out]["_bitrate"][i]/(region_count[region_out][data_out]["bit_time"][i])).toFixed(2)
                                                }
                                                if(output[region_out][data_out]["_freeze_rate"][i]>0 && count_temp!=0 )
                                                {
                                                    output[region_out][data_out]["_freeze_rate"][i]=(output[region_out][data_out]["_freeze_rate"][i]/(region_count[region_out][data_out]["sum"][i]-region_count[region_out][data_out]["center"][i])).toFixed(2)
                                                }
                                                if(output[region_out][data_out]["_success_rate"][i]>0)
                                                {
                                                   output[region_out][data_out]["_success_rate"][i]=(output[region_out][data_out]["_success_rate"][i]/region_count[region_out][data_out]["sum"][i]).toFixed(2)
                                                }
                                                output[region_out][data_out]["_band_width"]=(output[region_out][data_out]["_band_width"]-0).toFixed(2)
                                            }
                                        }
                                    }
                                }
                                var all_out_put={};
                                all_out_put["node"]=output;
                                all_out_put["all"]=all_output;
                                res.json(all_out_put);
                            }
                            else
                            {
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    db.close();
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

//获取按照地区的统计数据
var get_region_statistics_func = function(req,res){
    var output={};
    var all_out=[];
    var post_region=req.body.region;
    //var post_body = req.body.region;
    logger.info("post region");
    logger.info(post_region);
    //console.log("post_body".post_body);
    try{
        connect_mongo(res,function(db){
                db.collection('statistic_node',function(err,tb)
                {
                    if(!err)
                    {
                        var five_ago = new Date(new Date().getTime()-600000);
                        var start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                            parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                        //console.log(start)

                        var query = {
                            'time':1487903100,
                            "node_statistics.node.region":/天津.*/i
                        }
                        // var back = {
                        //     start:1,
                        //     s_ip:1,
                        //     band:1,
                        //     suc_r:1,
                        //     freeze_r:1,
                        //     bitrate:1,
                        //     _id:0
                        // }
                        //db.cntv_node_statistic_table.aggregate([{$project:{"node_statistics":1,'time':1,'_id':0}},{$unwind:"$node_statistics"},{$match:{'node_statistics.node.region':'天津','time':1487903100}}])
                        var back = {
                            _id:0,
                            "node_statistics.$":1
                        }
                        
                        //tb.find(query,back).toArray(function(err,logs)
                        var params_region =new RegExp( post_region ,"i");
                        // { liupan modify, 2018/3/8
                        // tb.aggregate([{$project:{"node_statistics":1,'time':1,'_id':0}},{$unwind:"$node_statistics"},{$match:{'node_statistics.node.region':params_region,'time':node_time_stamp}}]).toArray(function(err,logs)
                        query['time'] = node_time_stamp;
                        query['node_statistics.node.region'] = params_region;
                        // { liupan modify, 2018/3/21
                        // if (req.body.cdn != 'none') {
                        //     var cdn_list = req.body.cdn.split(",");
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            var cdn_list = req.body.cdn_list.split(",");
                        // } liupan modify, 2018/3/21
                            query['node_statistics.cdn'] = {$in: cdn_list};
                        }
                        tb.aggregate([{$project:{"node_statistics":1,'time':1,'_id':0}},{$unwind:"$node_statistics"},{$match:query}]).toArray(function(err,logs)
                        // } liupan modify, 2018/3/8
                        {
                            if(!err)
                            {
                                //res.json(logs)
                                //console.log(logs);
                                //var data=logs[0].node_statistics;
                                var data=logs;
                                var region_count={};
                                for(var i =0 ;i<data.length;i++)
                                {
                                    if(!data[i]["node_statistics"]["cdn"])
                                        continue;
                                    if(data[i]["node_statistics"].bitrate==0 && data[i]["node_statistics"].freeze_rate==0 && data[i]["node_statistics"].band_width==0 && data[i]["node_statistics"].success_rate==0)
                                    {    
                                        continue;
                                    }
                                    var vip_temp = data[i]["node_statistics"]["vip"];
                                    if(!output[data[i]["node_statistics"]["node"][0].operator])
                                    {
                                        output[data[i]["node_statistics"]["node"][0].operator]={};
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]={};
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bitrate"]=data[i]["node_statistics"]["bitrate"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bit_sum"]=data[i]["node_statistics"]["bit_sum"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bit_time"]=data[i]["node_statistics"]["bit_time"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_band_width"]=data[i]["node_statistics"]["band_width"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_freeze_rate"]=data[i]["node_statistics"]["freeze_rate"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_success_rate"]=data[i]["node_statistics"]["success_rate"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]={};
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_bitrate"]=data[i]["node_statistics"]["bitrate"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_bit_sum"]=data[i]["node_statistics"]["bit_sum"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_bit_time"]=data[i]["node_statistics"]["bit_time"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_band_width"]=data[i]["node_statistics"]["band_width"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_freeze_rate"]=data[i]["node_statistics"]["freeze_rate"]-0;
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_success_rate"]=data[i]["node_statistics"]["success_rate"]-0;
                                        region_count[data[i]["node_statistics"]["node"][0].operator]={};
                                        region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]={};
                                        region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["sum"]=1;
                                        region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["center"]=0;
                                        region_count[data[i]["node_statistics"]["node"][0].operator]["all"]={};
                                        region_count[data[i]["node_statistics"]["node"][0].operator]["all"]["sum"]=1;
                                        region_count[data[i]["node_statistics"]["node"][0].operator]["all"]["center"]=0;
                                        if((data[i]["node_statistics"]["level"]==3 || data[i]["node_statistics"]["level"]==4)&& vip_temp!=1)
                                        {
                                            region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["center"]+=1;
                                            region_count[data[i]["node_statistics"]["node"][0].operator]["all"]["center"]+=1;
                                        }
                                    }
                                    else
                                    {
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_bitrate"]+=data[i]["node_statistics"]["bitrate"];
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_bit_sum"]+=data[i]["node_statistics"]["bit_sum"];
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_bit_time"]+=data[i]["node_statistics"]["bit_time"];
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_band_width"]+=data[i]["node_statistics"]["band_width"];
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_freeze_rate"]+=data[i]["node_statistics"]["freeze_rate"];
                                        output[data[i]["node_statistics"]["node"][0].operator]["all"]["_success_rate"]+=data[i]["node_statistics"]["success_rate"];
                                        region_count[data[i]["node_statistics"]["node"][0].operator]["all"]["sum"]+=1;
                                        if(!output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]])
                                        {
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]={};
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bitrate"]=data[i]["node_statistics"]["bitrate"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bit_sum"]=data[i]["node_statistics"]["bit_sum"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bit_time"]=data[i]["node_statistics"]["bit_time"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_band_width"]=data[i]["node_statistics"]["band_width"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_freeze_rate"]=data[i]["node_statistics"]["freeze_rate"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_success_rate"]=data[i]["node_statistics"]["success_rate"];
                                            region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]={};
                                            region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["sum"]=1;
                                            region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["center"]=0;

                                            if((data[i]["node_statistics"]["level"]==3 || data[i]["node_statistics"]["level"]==4)&& vip_temp!=1)
                                            {
                                                region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["center"]+=1;
                                                region_count[data[i]["node_statistics"]["node"][0].operator]["all"]["center"]+=1;
                                            }
                                        }
                                        else
                                        {
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bitrate"]+=data[i]["node_statistics"]["bitrate"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bit_sum"]+=data[i]["node_statistics"]["bit_sum"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_bit_time"]+=data[i]["node_statistics"]["bit_time"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_band_width"]+=data[i]["node_statistics"]["band_width"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_freeze_rate"]+=data[i]["node_statistics"]["freeze_rate"];
                                            output[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["_success_rate"]+=data[i]["node_statistics"]["success_rate"];
                                            region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["sum"]+=1;
                                            if((data[i]["node_statistics"]["level"]==3 || data[i]["node_statistics"]["level"]==4)&& vip_temp!=1)
                                            {
                                                region_count[data[i]["node_statistics"]["node"][0].operator][data[i]["node_statistics"]["cdn"]]["center"]+=1;
                                                region_count[data[i]["node_statistics"]["node"][0].operator]["all"]["center"]+=1;
                                            }
                                        }
                                    }
                                }

                                for(var operator_out in output)
                                {
                                    for (var data_out in output[operator_out])
                                    {
                                        if(output[operator_out][data_out]._bitrate!=0)
                                        {
                                            //output[operator_out][data_out]._bitrate=(output[operator_out][data_out]._bitrate/(region_count[operator_out][data_out].sum-region_count[operator_out][data_out].center)).toFixed(2);
                                            output[operator_out][data_out]._bitrate=output[operator_out][data_out]._bit_sum/output[operator_out][data_out]._bit_time;
                                        }
                                        if(output[operator_out][data_out]._freeze_rate!=0)
                                        {
                                            output[operator_out][data_out]._freeze_rate=(output[operator_out][data_out]._freeze_rate/(region_count[operator_out][data_out].sum-region_count[operator_out][data_out].center)).toFixed(2);
                                        }
                                        if(output[operator_out][data_out]._success_rate!=0)
                                        {
                                           output[operator_out][data_out]._success_rate=(output[operator_out][data_out]._success_rate/region_count[operator_out][data_out].sum).toFixed(2);
                                        }
                                        output[operator_out][data_out]._bitrate=(output[operator_out][data_out]._bitrate-0).toFixed(2);
                                    }
                                }


                                //all_out.push(output);
                                //all_out.push(region_count);
                                //all_out.push(logs);
                                res.json(output);
                                //console.log(output);
                                //res.json(all_out);
                            }
                            else
                            {
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    db.close()
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

var get_region_user_statistics_func = function(req,res){
    var output={};
    var post_region=req.body.region;
    //var post_body = req.body.region;
    logger.info("get_region_user_statistics_func");
    logger.info(post_region);
    //console.log("post_body".post_body);
    try{
        connect_mongo(res,function(db){
                db.collection('statistic_user',function(err,tb)
                {
                    if(!err)
                    {
                        var five_ago = new Date(new Date().getTime()-600000);
                        var start = parseInt(
                            new Date(five_ago.getFullYear(),five_ago.getMonth(),five_ago.getDate(),five_ago.getHours(),
                            parseInt(five_ago.getMinutes()/5)*5,0).getTime()/1000
                        )
                        //console.log(start)

                        var query = {
                            // 'time':1487903100,
                            // "node_statistics.node.region":/天津.*/i
                        }
                        var back = {
                            _id:0,
                            "node_statistics.$":1
                        }
                        //tb.find(query,back).toArray(function(err,logs)
                        var params_region =new RegExp( post_region ,"i");
                        //console.log(params_region);
                        // { liupan modify, 2018/3/8
                        // tb.aggregate([{$project:{"user_statistics":1,'time':1,'_id':0}},{$unwind:"$user_statistics"},{$match:{'user_statistics.region':params_region,'time':user_time_stamp}}]).toArray(function(err,logs)
                        query['time'] = user_time_stamp;
                        query['user_statistics.region'] = params_region;
                        // { liupan modify, 2018/3/21
                        // if (req.body.cdn != 'none') {
                        //     var cdn_list = req.body.cdn.split(",");
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            var cdn_list = req.body.cdn_list.split(",");
                        // } liupan modify, 2018/3/21
                            query['user_statistics.cdn'] = {$in: cdn_list};
                        }
                        tb.aggregate([{$project:{"user_statistics":1,'time':1,'_id':0}},{$unwind:"$user_statistics"},{$match:query}]).toArray(function(err,logs)
                        // } liupan modify, 2018/3/8
                        {
                            if(!err)
                            {
                                // res.json(logs);
                                // console.log(logs);
                                // return;
                                //var data=logs[0].node_statistics;                                
                                var region_count={};
                                //res.json(data);
                                //return;
                                //for(var i =0 ;i<data.length;i++)
                                for(var i=0;i<logs.length;i++)
                                {
                                    var data=logs[i].user_statistics;
                                    var operator_temp=data["operator"];
                                    var cdn_temp=data["cdn"];
                                    var level_temp=data["level"];
                                    var vip_temp = data["vip"];
                                    if(!output[operator_temp])
                                    {
                                        output[operator_temp]={};
                                        output[operator_temp][cdn_temp]={};
                                        output[operator_temp][cdn_temp]["_bitrate"]=data["bit_sum"]-0;
                                        output[operator_temp][cdn_temp]["_band_width"]=data["band_width"]-0;
                                        output[operator_temp][cdn_temp]["_freeze_rate"]=data["freeze_rate"]-0;
                                        output[operator_temp][cdn_temp]["_success_rate"]=data["success_rate"]-0;
                                        output[operator_temp]["all"]={};
                                        output[operator_temp]["all"]["_bitrate"]=data["bit_sum"]-0;
                                        output[operator_temp]["all"]["_band_width"]=data["band_width"]-0;
                                        output[operator_temp]["all"]["_freeze_rate"]=data["freeze_rate"]-0;
                                        output[operator_temp]["all"]["_success_rate"]=data["success_rate"]-0;
                                        region_count[operator_temp]={};
                                        region_count[operator_temp][cdn_temp]={"sum":1,"center":0,'bit_time':data["bit_time"]};
                                        // region_count[operator_temp][cdn_temp]["sum"]=1;
                                        // region_count[operator_temp][cdn_temp]["center"]=0;
                                        region_count[operator_temp]["all"]={"sum":1,"center":0,'bit_time':data["bit_time"]};
                                        // region_count[operator_temp]["all"]["sum"]=1;
                                        // region_count[operator_temp]["all"]["center"]=0;
                                        if((level_temp==3 || level_temp==4)&& vip_temp!=1)
                                        {
                                            region_count[operator_temp][cdn_temp]["center"]+=1;
                                            region_count[operator_temp]["all"]["center"]+=1;
                                        }
                                    }
                                    else
                                    {
                                        output[operator_temp]["all"]["_bitrate"]+=data["bit_sum"];
                                        output[operator_temp]["all"]["_band_width"]+=data["band_width"];
                                        output[operator_temp]["all"]["_freeze_rate"]+=data["freeze_rate"];
                                        output[operator_temp]["all"]["_success_rate"]+=data["success_rate"];
                                        region_count[operator_temp]["all"]["sum"]+=1;
                                        region_count[operator_temp]["all"]["bit_time"]+=data["bit_time"];
                                        if(!output[operator_temp][cdn_temp])
                                        {
                                            output[operator_temp][cdn_temp]={};
                                            output[operator_temp][cdn_temp]["_bitrate"]=data["bit_sum"];
                                            output[operator_temp][cdn_temp]["_band_width"]=data["band_width"];
                                            output[operator_temp][cdn_temp]["_freeze_rate"]=data["freeze_rate"];
                                            output[operator_temp][cdn_temp]["_success_rate"]=data["success_rate"];
                                            region_count[operator_temp][cdn_temp]={"sum":1,"center":0,'bit_time':data["bit_time"]};
                                            if((level_temp==3 || level_temp==4)&& vip_temp!=1)
                                            {
                                                region_count[operator_temp][cdn_temp]["center"]+=1;
                                                region_count[operator_temp]["all"]["center"]+=1;
                                            }
                                        }
                                        else
                                        {
                                            output[operator_temp][cdn_temp]["_bitrate"]+=data["bit_sum"];
                                            output[operator_temp][cdn_temp]["_band_width"]+=data["band_width"];
                                            output[operator_temp][cdn_temp]["_freeze_rate"]+=data["freeze_rate"];
                                            output[operator_temp][cdn_temp]["_success_rate"]+=data["success_rate"];
                                            region_count[operator_temp][cdn_temp]["sum"]+=1;
                                            region_count[operator_temp][cdn_temp]["bit_time"]+=data["bit_time"];
                                            if((level_temp==3 || level_temp==4)&& vip_temp!=1)
                                            {
                                                region_count[operator_temp][cdn_temp]["center"]+=1;
                                                region_count[operator_temp]["all"]["center"]+=1;
                                            }
                                        }
                                    }
                                }

                                for(var operator_out in output)
                                {
                                    for (var data_out in output[operator_out])
                                    {
                                        var count_temp=(region_count[operator_out][data_out].sum-region_count[operator_out][data_out].center);
                                        if(output[operator_out][data_out]._bitrate!=0)
                                        {
                                            output[operator_out][data_out]._bitrate=(output[operator_out][data_out]._bitrate/(region_count[operator_out][data_out].bit_time)).toFixed(2);
                                        }
                                        if(output[operator_out][data_out]._freeze_rate!=0 && count_temp!=0)
                                        {
                                            output[operator_out][data_out]._freeze_rate=(output[operator_out][data_out]._freeze_rate/(region_count[operator_out][data_out].sum-region_count[operator_out][data_out].center)).toFixed(2);
                                        }
                                        if(output[operator_out][data_out]._success_rate!=0)
                                        {
                                           output[operator_out][data_out]._success_rate=(output[operator_out][data_out]._success_rate/region_count[operator_out][data_out].sum).toFixed(2);
                                        }
                                        output[operator_out][data_out]._bitrate=(output[operator_out][data_out]._bitrate-0).toFixed(2);
                                    }
                                }
                                res.json(output);
                            }
                            else
                            {
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    db.close()
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

//按照地区、运营商、cdn参数来获取详细信息。
var get_detail_statistics_func = function(req,res){
    var output={};
    var post_region=req.body.region;
    var post_cdn=req.body.cdn;
    var post_operator=req.body.operator;
    //var post_body = req.body.region;
    logger.info("post region");
    logger.info(post_region);
    logger.info(post_operator);
    logger.info(post_cdn);
    try{
        connect_mongo(res,function(db){
                db.collection('statistic_node',function(err,tb)
                {
                    if(!err)
                    {
                        var query = {
                            'time':1487903100,
                            "node_statistics.node.region":/天津.*/i
                        }
                        
                        var params_region =new RegExp( post_region ,"i");
                        var logs={};
                        tb.aggregate([{$project:{"node_statistics":1,'time':1,'_id':0}},{$unwind:"$node_statistics"},{$match:{'node_statistics.node.region':params_region,'node_statistics.cdn':post_cdn,'node_statistics.node.operator':post_operator,'time':node_time_stamp}}]).toArray(function(err,logs)
                        {
                            if(!err)
                            {

                                //res.json(logs)
                                //console.log(logs);
                                //var data=logs[0].node_statistics;

                                var data=logs;

                                var region_count={};
                                for(var i =0 ;i<data.length;i++)
                                {
                                    output[data[i]["node_statistics"]["node"][0].ip]={};
                                    output[data[i]["node_statistics"]["node"][0].ip]["_bitrate"]=data[i]["node_statistics"]["bitrate"];
                                    output[data[i]["node_statistics"]["node"][0].ip]["_band_width"]=data[i]["node_statistics"]["band_width"];
                                    output[data[i]["node_statistics"]["node"][0].ip]["_freeze_rate"]=data[i]["node_statistics"]["freeze_rate"];
                                    output[data[i]["node_statistics"]["node"][0].ip]["_success_rate"]=data[i]["node_statistics"]["success_rate"];
                                }

                                // var all_out=[];
                                // all_out.push(logs);
                                // all_out.push(output);
                                //all_out.push(region_count);
                                //all_out.push(logs);
                                //all_out.push(logs2);
                                res.json(output);
                                //console.log(output);
                                //res.json(all_out);
                                db.close();
                                return;
                            }
                            else
                            {
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    db.close();
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

var get_detail_user_statistics_func = function(req,res){
    var output={};
    var post_region=req.body.region;
    var post_cdn=req.body.cdn;
    var post_operator=req.body.operator;
    //var post_body = req.body.region;
    logger.info("post region");
    logger.info(post_region);
    logger.info(post_operator);
    logger.info(post_cdn);
    try{
        connect_mongo(res,function(db){
                db.collection('statistic_user',function(err,tb)
                {
                    if(!err)
                    {

                        var params_region =new RegExp( post_region ,"i");
                        var logs={};
                        tb.aggregate([{$project:{"user_statistics":1,'time':1,'_id':0}},{$unwind:"$user_statistics"},{$match:{'user_statistics.region':params_region,'user_statistics.cdn':post_cdn,'user_statistics.operator':post_operator,'time':user_time_stamp}}]).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                var data=logs;
                                // res.json(logs);
                                // db.close();
                                // return;
                                var ip_count={};
                                var region_count={};
                                for(var j =0 ;j<data.length;j++)
                                {
                                    var ip_temp=data[j]["user_statistics"].ip;
                                    //console.log(ip_temp);
                                    for(var i =0 ;i<ip_temp.length;i++)
                                    {
                                        if(ip_temp[i]["bitrate"]==0 && ip_temp[i]["band_width"]==0 && ip_temp[i]["freeze_rate"]==0 && ip_temp[i]["success_rate"]==0)
                                        {
                                            continue;
                                        }
                                        if (!output[ip_temp[i].ip])
                                        {
                                            if(ip_temp[i]["bitrate"]==0 && ip_temp[i]["freeze_rate"]==0)
                                            {
                                                ip_count[ip_temp[i].ip]={"_bitrate":0,"_freeze_rate":0,"_success_rate":1};
                                            }
                                            else
                                            {
                                                ip_count[ip_temp[i].ip]={"_bitrate":1,"_freeze_rate":1,"_success_rate":1};
                                            }
                                            ip_count[ip_temp[i].ip]['bit_time']=ip_temp[i]["bit_time"];
                                            output[ip_temp[i].ip]={"_bitrate":0,"_band_width":0,"_freeze_rate":0,"_success_rate":0};
                                            output[ip_temp[i].ip]["_bitrate"]=ip_temp[i]["bit_sum"];
                                            output[ip_temp[i].ip]["_band_width"]=ip_temp[i]["band_width"];
                                            output[ip_temp[i].ip]["_freeze_rate"]=ip_temp[i]["freeze_rate"];
                                            output[ip_temp[i].ip]["_success_rate"]=ip_temp[i]["success_rate"];
                                        }
                                        else
                                        {
                                            if(output[ip_temp[i].ip]["_bitrate"]==0 && output[ip_temp[i].ip]["_freeze_rate"]==0)
                                            {
                                                ip_count[ip_temp[i].ip]['_success_rate']+=1;
                                            }
                                            else
                                            {
                                                ip_count[ip_temp[i].ip]['_bitrate']+=1;
                                                ip_count[ip_temp[i].ip]['_freeze_rate']+=1;
                                                ip_count[ip_temp[i].ip]['_success_rate']+=1;
                                            }
                                            ip_count[ip_temp[i].ip]['bit_time']+=ip_temp[i]["bit_time"];
                                            output[ip_temp[i].ip]["_bitrate"]+=ip_temp[i]["bit_sum"];
                                            output[ip_temp[i].ip]["_band_width"]+=ip_temp[i]["band_width"];
                                            output[ip_temp[i].ip]["_freeze_rate"]+=ip_temp[i]["freeze_rate"];
                                            output[ip_temp[i].ip]["_success_rate"]+=ip_temp[i]["success_rate"];
                                        }
                                    }
                                }
                                for(var _ip in output)
                                {
                                    if(ip_count[_ip]["bit_time"]!=0)
                                    {
                                        output[_ip]["_bitrate"]=output[_ip]["_bitrate"]/ip_count[_ip]["bit_time"];
                                    }
                                    if(ip_count[_ip]["_freeze_rate"]!=0)
                                    {
                                        output[_ip]["_freeze_rate"]=output[_ip]["_freeze_rate"]/ip_count[_ip]["_freeze_rate"];
                                    }
                                    if(ip_count[_ip]["_success_rate"]!=0)
                                    {
                                        output[_ip]["_success_rate"]=output[_ip]["_success_rate"]/ip_count[_ip]["_success_rate"];
                                    }
                                }
                                var all_out=[];
                                all_out.push(output);
                                all_out.push(logs);
                                res.json(output);
                                db.close();
                                return;
                            }
                            else
                            {
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                    db.close()
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}


//获取特殊地区的数据。
var get_special_region_statistics_func = function(req,res){

    var post_region=req.body.region;
    var post_cdn=req.body.cdn;
    var post_operator=req.body.operator;
    //console.log("post_body".post_body);
   // try{
        var all_out=[];
        // console.log(post_region.length);
        var region_arr=JSON.parse(post_region);
        // var region_arr=post_region;
        region_arr=eval(region_arr);
        //console.log(region_arr+region_arr.length);
        var t=0;
        async.map(region_arr,function(item,callback)
        {
            var output={};
            connect_mongo(res, function(db) {
                db.collection('statistic_node', function(err, tb) 
                {
                    if (!err) {
                        var five_ago = new Date(new Date().getTime() - 600000);
                        var start = parseInt(
                          new Date(five_ago.getFullYear(), five_ago.getMonth(), five_ago.getDate(), five_ago.getHours(),
                            parseInt(five_ago.getMinutes() / 5) * 5, 0).getTime() / 1000
                        );

                        var city_temp = item.city + '';
                        var params_region = new RegExp(item.city, "i");
                        var match_params={};
                        if(item.city == 'vip')
                        {
                            match_params={'time': node_time_stamp, 'node_statistics.vip': 1 }
                        }
                        else
                        {
                            match_params={ 'node_statistics.node.region': params_region, 'time': node_time_stamp, 'node_statistics.vip': 0 }
                        }
                        if(req.body.cdn != "none")
                        {
                            match_params["node_statistics.cdn"] = req.body.cdn;
                        }
                        // { liupan add, 2018/3/21
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            var cdn_list = req.body.cdn_list.split(",");
                            match_params["node_statistics.cdn"] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/21
                        //console.log('region='+item.city);
                        //console.log(match_params);
                        tb.aggregate([{ $project: { "node_statistics": 1, 'time': 1, '_id': 0 } }, { $unwind: "$node_statistics" }, { $match:match_params}]).toArray(function(err, logs) {
                            if (!err)
                            {
                                var data = logs;
                                var region_count = {};
                                for (var i = 0; i < data.length; i++) {
                                    var region_temp = data[i]["node_statistics"].node[0].region + '';
                                    var cdn_temp = data[i]["node_statistics"].cdn + '';
                                    //var region_ret = region_temp.indexOf(item.city);
                                    var region_ret = 1;
                                    var level_temp = data[i]["node_statistics"].level;
                                    var operator_temp = data[i]["node_statistics"].node[0].operator + '';
                                    var vip_temp = data[i]["node_statistics"]["vip"];
                                    //if (region_ret!=-1)
                                    // if(city_temp=='北京')
                                    // {
                                    //   console.log(i+data);
                                    // }
                                    {
                                        if(data[i]["node_statistics"].bitrate==0 && data[i]["node_statistics"].freeze_rate==0 && data[i]["node_statistics"].band_width==0 && data[i]["node_statistics"].success_rate==0)
                                        {
                                            continue;
                                        }
                                        if (output[city_temp]) 
                                        {
                                            output[item.city]["all"]["_bitrate"] += data[i]["node_statistics"].bitrate-0;
                                            output[item.city]["all"]["_bit_sum"] += data[i]["node_statistics"].bit_sum-0;
                                            output[item.city]["all"]["_bit_time"] += data[i]["node_statistics"].bit_time-0;
                                            output[item.city]["all"]["_freeze_rate"] += data[i]["node_statistics"].freeze_rate;
                                            output[item.city]["all"]["_band_width"] += data[i]["node_statistics"].band_width;
                                            output[item.city]["all"]["_success_rate"] += data[i]["node_statistics"].success_rate;
                                            region_count[item.city]["all"]["sum"] += 1;
                                            //if(cdn_temp)
                                            {
                                            if (output[item.city][cdn_temp]) 
                                            {
                                                region_count[item.city][cdn_temp]["sum"][0] += 1;
                                                region_count[item.city][cdn_temp]["sum"][operator_locate[operator_temp]] += 1;
                                                if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                                {
                                                    region_count[item.city]["all"]["center"] += 1;
                                                    region_count[item.city][cdn_temp]["center"][operator_locate[operator_temp]] += 1;
                                                    region_count[item.city][cdn_temp]["center"][0] += 1;
                                                }
                                                if (output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] == -1) {
                                                    output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] = data[i]["node_statistics"].bitrate;
                                                    output[item.city][cdn_temp]["_bit_sum"][operator_locate[operator_temp]] = data[i]["node_statistics"].bit_sum;
                                                    output[item.city][cdn_temp]["_bit_time"][operator_locate[operator_temp]] = data[i]["node_statistics"].bit_time;
                                                    //output[item.city][cdn_temp]["_bitrate"][0] += data[i]["node_statistics"].bitrate-0;
                                                } else {
                                                    output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] += data[i]["node_statistics"].bitrate;
                                                    output[item.city][cdn_temp]["_bit_sum"][operator_locate[operator_temp]] += data[i]["node_statistics"].bit_sum;
                                                    output[item.city][cdn_temp]["_bit_time"][operator_locate[operator_temp]] += data[i]["node_statistics"].bit_time;
                                                    //output[item.city][cdn_temp]["_bitrate"][0] += data[i]["node_statistics"].bitrate-0;
                                                }
                                                output[item.city][cdn_temp]["_bitrate"][0] += data[i]["node_statistics"].bitrate-0;
                                                output[item.city][cdn_temp]["_bit_sum"][0] += data[i]["node_statistics"].bit_sum-0;
                                                output[item.city][cdn_temp]["_bit_time"][0] += data[i]["node_statistics"].bit_time-0;

                                                if (output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] == -1) {
                                                    output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] = data[i]["node_statistics"].freeze_rate
                                                    output[item.city][cdn_temp]["_freeze_rate"][0] += data[i]["node_statistics"].freeze_rate
                                                } else {
                                                    output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] += data[i]["node_statistics"].freeze_rate
                                                    output[item.city][cdn_temp]["_freeze_rate"][0] += data[i]["node_statistics"].freeze_rate
                                                }

                                                if (output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] == -1) {
                                                    output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] = data[i]["node_statistics"].success_rate;
                                                    output[item.city][cdn_temp]["_success_rate"][0] += data[i]["node_statistics"].success_rate;
                                                } else {
                                                    output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] += data[i]["node_statistics"].success_rate;
                                                    output[item.city][cdn_temp]["_success_rate"][0] += data[i]["node_statistics"].success_rate;
                                                }

                                                output[item.city][cdn_temp]["_band_width"] += data[i]["node_statistics"].band_width;
                                            } 
                                            else {
                                                region_count[item.city][cdn_temp] = {};
                                                region_count[item.city][cdn_temp]["sum"] = [1, 0, 0, 0, 0];
                                                region_count[item.city][cdn_temp]["sum"][operator_locate[operator_temp]] = 1;
                                                region_count[item.city][cdn_temp]["center"] = [0, 0, 0, 0, 0];
                                            if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1)
                                            {
                                                region_count[item.city]["all"]["center"] += 1;
                                                region_count[item.city][cdn_temp]["center"][operator_locate[operator_temp]] = 1;
                                                region_count[item.city][cdn_temp]["center"][0] = 1;
                                            } 
                                            else 
                                            {
                                                region_count[item.city]["all"]["center"] += 0;
                                            }
                                                output[item.city][cdn_temp] = {};
                                                output[item.city][cdn_temp]["_bitrate"] = [data[i]["node_statistics"].bitrate, -1, -1, -1, -1];
                                                output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] = data[i]["node_statistics"].bitrate;
                                                output[item.city][cdn_temp]["_bit_sum"] = [data[i]["node_statistics"].bit_sum, -1, -1, -1, -1];
                                                output[item.city][cdn_temp]["_bit_sum"][operator_locate[operator_temp]] = data[i]["node_statistics"].bit_sum;
                                                output[item.city][cdn_temp]["_bit_time"] = [data[i]["node_statistics"].bit_time, -1, -1, -1, -1];
                                                output[item.city][cdn_temp]["_bit_time"][operator_locate[operator_temp]] = data[i]["node_statistics"].bit_time;

                                                output[item.city][cdn_temp]["_freeze_rate"] = [data[i]["node_statistics"].freeze_rate, -1, -1, -1, -1];
                                                output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] = data[i]["node_statistics"].freeze_rate;
                                                output[item.city][cdn_temp]["_band_width"] = data[i]["node_statistics"].band_width;
                                                output[item.city][cdn_temp]["_success_rate"] = [data[i]["node_statistics"].success_rate, -1, -1, -1, -1];
                                                output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] = data[i]["node_statistics"].success_rate;
                                            }
                                          }
                                        } else {
                                          // console.log("item");
                                          // console.log(item.city);
                                          region_count[item.city] = {};
                                          region_count[item.city]["all"] = {};
                                          region_count[item.city]["all"]["sum"] = 1;
                                          region_count[item.city][cdn_temp] = {};
                                          //region_count[item.city][cdn_temp]["sum"] = [];
                                          region_count[item.city][cdn_temp]["center"] = [0, 0, 0, 0, 0];
                                          region_count[item.city][cdn_temp]["sum"] = [1, 0, 0, 0, 0];
                                          region_count[item.city][cdn_temp]["sum"][operator_locate[operator_temp]] = 1;
                                          if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1) {
                                            region_count[item.city]["all"]["center"] = 1;
                                            region_count[item.city][cdn_temp]["center"][operator_locate[operator_temp]] = 1;
                                            region_count[item.city][cdn_temp]["center"][0] = 1;
                                          } else {
                                            region_count[item.city]["all"]["center"] = 0;
                                          }
                                          output[item.city] = {};
                                          output[item.city]["all"] = {};
                                          output[item.city]["all"]["_bitrate"] = data[i]["node_statistics"].bitrate;
                                          output[item.city]["all"]["_bit_sum"] = data[i]["node_statistics"].bit_sum;
                                          output[item.city]["all"]["_bit_time"] = data[i]["node_statistics"].bit_time;
                                          output[item.city]["all"]["_freeze_rate"] = data[i]["node_statistics"].freeze_rate;
                                          output[item.city]["all"]["_band_width"] = data[i]["node_statistics"].band_width;
                                          output[item.city]["all"]["_success_rate"] = data[i]["node_statistics"].success_rate;
                                          output[item.city][cdn_temp] = {};
                                          output[item.city][cdn_temp]["_bitrate"] = [data[i]["node_statistics"].bitrate, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] = data[i]["node_statistics"].bitrate;
                                          output[item.city][cdn_temp]["_bit_sum"] = [data[i]["node_statistics"].bit_sum, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_bit_sum"][operator_locate[operator_temp]] = data[i]["node_statistics"].bit_sum;
                                          output[item.city][cdn_temp]["_bit_time"] = [data[i]["node_statistics"].bit_time, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_bit_time"][operator_locate[operator_temp]] = data[i]["node_statistics"].bit_time;

                                          output[item.city][cdn_temp]["_freeze_rate"] = [data[i]["node_statistics"].freeze_rate, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] = data[i]["node_statistics"].freeze_rate;
                                          output[item.city][cdn_temp]["_band_width"] = data[i]["node_statistics"].band_width;
                                          output[item.city][cdn_temp]["_success_rate"] = [data[i]["node_statistics"].success_rate, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] = data[i]["node_statistics"].success_rate;
                                        }
                                      }
                                }
                                //console.log(output['北京'])
                                for (var region_out in output) {
                                      for (var data_out in output[region_out]) {
                                        if (data_out == "all") {
                                          if (output[region_out][data_out]._bit_time > 0) {
                                            //output[region_out][data_out]._bitrate = (output[region_out][data_out]._bitrate / (region_count[region_out][data_out].sum - region_count[region_out][data_out].center)).toFixed(2)
                                            output[region_out][data_out]._bitrate = (output[region_out][data_out]._bit_sum / output[region_out][data_out]._bit_time).toFixed(2);
                                          }
                                          if (output[region_out][data_out]._freeze_rate > 0 && (region_count[region_out][data_out].sum - region_count[region_out][data_out].center) > 0) {
                                            output[region_out][data_out]._freeze_rate = (output[region_out][data_out]._freeze_rate / (region_count[region_out][data_out].sum - region_count[region_out][data_out].center)).toFixed(2)
                                          }
                                          if (output[region_out][data_out]._success_rate > 0) {
                                            output[region_out][data_out]._success_rate = (output[region_out][data_out]._success_rate / region_count[region_out][data_out].sum).toFixed(2)
                                          }
                                          output[region_out][data_out]._band_width = (output[region_out][data_out]._band_width - 0).toFixed(2)
                                        } else {
                                          for (var i = 0; i < 6; i++) {
                                            if (output[region_out][data_out]["_bit_time"][i] > 0) {
                                              //output[region_out][data_out]["_bitrate"][i] = (output[region_out][data_out]["_bitrate"][i] / (region_count[region_out][data_out]["sum"][i] - region_count[region_out][data_out]["center"][i])).toFixed(2)
                                              output[region_out][data_out]["_bitrate"][i] = (output[region_out][data_out]["_bit_sum"][i] / output[region_out][data_out]["_bit_time"][i] ).toFixed(2);
                                            }
                                            if (output[region_out][data_out]["_freeze_rate"][i] > 0 && (region_count[region_out][data_out]["sum"][i] - region_count[region_out][data_out]["center"][i])!=0) {
                                              output[region_out][data_out]["_freeze_rate"][i] = (output[region_out][data_out]["_freeze_rate"][i] / (region_count[region_out][data_out]["sum"][i] - region_count[region_out][data_out]["center"][i])).toFixed(2)
                                            }
                                            if (output[region_out][data_out]["_success_rate"][i] > 0 ) {
                                              output[region_out][data_out]["_success_rate"][i] = (output[region_out][data_out]["_success_rate"][i] / region_count[region_out][data_out]["sum"][i]).toFixed(2)
                                            }
                                            output[region_out][data_out]["_band_width"] = (output[region_out][data_out]["_band_width"] - 0).toFixed(2)
                                          }
                                        }
                                      }
                                };
                                //console.log(output);

                                db.close()
                                // console.log(region_count['北京']['kw']['sum']);
                                // console.log(region_count['北京']['kw']['center']);
                                callback(null,output);

                            } else {

                                // res.json({
                                //     ErrNo: "102",
                                //     ErrMsg: "Failed to get logs"
                                // });
                                db.close();
                                return
                            }
                        })
                        delete params_region;

                    } else {
                        res.json({
                            Err: err,
                            ErrNo: "101",
                            ErrMsg: "Failed to get table"
                          })
                        }
                        db.close();
                        return;
                    });

              });
            // console.log(output);
            // callback(null,output);
        },
        function(err,results)
        {
            // console.log("huaqings"+results);
            res.json(results);
        });
    // }catch(e){
    //     res.json({
    //         ErrNo:"100",
    //         ErrMsg:"数据库错误"
    //     })
    // }
}

var get_special_region_user_statistics_func = function(req,res){

    var post_region=req.body.region;
    var post_cdn=req.body.cdn;
    var post_operator=req.body.operator;

    //console.log(req.body);
    //console.log("post_body".post_body);
   // try{
        var all_out=[];
        // console.log(post_region.length);
        var region_arr=JSON.parse(post_region);
        // var region_arr=post_region;
        region_arr=eval(region_arr);
        //console.log(region_arr+region_arr.length);
        var t=0;
        async.map(region_arr,function(item,callback)
        {
            var output={};
            connect_mongo(res, function(db) {
                db.collection('statistic_user', function(err, tb) 
                {
                    if (!err) {
                        var five_ago = new Date(new Date().getTime() - 600000);
                        var start = parseInt(
                          new Date(five_ago.getFullYear(), five_ago.getMonth(), five_ago.getDate(), five_ago.getHours(),
                            parseInt(five_ago.getMinutes() / 5) * 5, 0).getTime() / 1000
                        );

                        var city_temp = item.city + '';
                        var params_region = new RegExp(item.city, "i");
                        //console.log("item city"+item.city+"vip "+item.vip);
                        //console.log(params_region);
                        if(item.city == 'vip')
                        {
                            match_params={ 'time': user_time_stamp, 'user_statistics.vip': 1 }
                        }
                        else
                        {
                            match_params={ 'user_statistics.region': params_region, 'time': user_time_stamp, 'user_statistics.vip': 0 }
                        }
                        if( req.body.cdn != "none" )
                        {
                            match_params["user_statistics.cdn"] = req.body.cdn;
                        }
                        // { liupan add, 2018/3/21
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            var cdn_list = req.body.cdn_list.split(",");
                            match_params["user_statistics.cdn"] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/21
                        //console.log('region='+item.city);
                        //console.log(match_params);
                        tb.aggregate([{ $project: { "user_statistics": 1, 'time': 1, '_id': 0 } }, { $unwind: "$user_statistics" }, { $match:match_params  }]).toArray(function(err, logs) {
                            if (!err) {
                                //console.log(logs);
                                var data = logs;
                                var region_count = {};
                                for (var i = 0; i < data.length; i++) {
                                    var region_temp = data[i]["user_statistics"].region + '';
                                    var cdn_temp = data[i]["user_statistics"].cdn + '';
                                    //var region_ret = region_temp.indexOf(item.city);
                                    var region_ret = 1;
                                    var level_temp = data[i]["user_statistics"].level;
                                    var operator_temp = data[i]["user_statistics"].operator + '';
                                    var vip_temp = data[i]["user_statistics"]["vip"];
                                    {
                                        if (output[city_temp]) {
                                          output[item.city]["all"]["_bitrate"] += data[i]["user_statistics"].bit_sum-0;
                                          output[item.city]["all"]["_freeze_rate"] += data[i]["user_statistics"].freeze_rate;
                                          output[item.city]["all"]["_band_width"] += data[i]["user_statistics"].band_width;
                                          output[item.city]["all"]["_success_rate"] += data[i]["user_statistics"].success_rate;
                                          region_count[item.city]["all"]["sum"] += 1;
                                          region_count[item.city]["all"]["bit_time"]+=data[i]["user_statistics"].bit_time
                                          //if(cdn_temp)
                                          {
                                            if (output[item.city][cdn_temp]) {
                                              region_count[item.city][cdn_temp]["sum"][0] += 1;
                                              region_count[item.city][cdn_temp]["sum"][operator_locate[operator_temp]] += 1;
                                              region_count[item.city][cdn_temp]["bit_time"][0] += data[i]["user_statistics"].bit_time;
                                              region_count[item.city][cdn_temp]["bit_time"][operator_locate[operator_temp]] += data[i]["user_statistics"].bit_time;
                                              if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1) {
                                                region_count[item.city]["all"]["center"] += 1;
                                                region_count[item.city][cdn_temp]["center"][operator_locate[operator_temp]] += 1;
                                                region_count[item.city][cdn_temp]["center"][0] += 1;
                                              }
                                              if (output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] == -1) {
                                                output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] = data[i]["user_statistics"].bit_sum;
                                                output[item.city][cdn_temp]["_bitrate"][0] += data[i]["user_statistics"].bit_sum-0;
                                              } else {
                                                output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] += data[i]["user_statistics"].bit_sum;
                                                output[item.city][cdn_temp]["_bitrate"][0] += data[i]["user_statistics"].bit_sum-0;
                                              }
                                              if (output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] == -1) {
                                                output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] = data[i]["user_statistics"].freeze_rate
                                                output[item.city][cdn_temp]["_freeze_rate"][0] += data[i]["user_statistics"].freeze_rate
                                              } else {
                                                output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] += data[i]["user_statistics"].freeze_rate
                                                output[item.city][cdn_temp]["_freeze_rate"][0] += data[i]["user_statistics"].freeze_rate
                                              }
                                              if (output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] == -1) {
                                                output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] = data[i]["user_statistics"].success_rate;
                                                output[item.city][cdn_temp]["_success_rate"][0] += data[i]["user_statistics"].success_rate;
                                              } else {
                                                output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] += data[i]["user_statistics"].success_rate;
                                                output[item.city][cdn_temp]["_success_rate"][0] += data[i]["user_statistics"].success_rate;
                                              }
                                              output[item.city][cdn_temp]["_band_width"] += data[i]["user_statistics"].band_width;
                                            } else {
                                              region_count[item.city][cdn_temp] = {};
                                              region_count[item.city][cdn_temp]["sum"] = [1, 0, 0, 0, 0];
                                              region_count[item.city][cdn_temp]["sum"][operator_locate[operator_temp]] = 1;
                                              region_count[item.city][cdn_temp]["bit_time"] = [data[i]["user_statistics"].bit_time, 0, 0, 0, 0];
                                              region_count[item.city][cdn_temp]["bit_time"][operator_locate[operator_temp]] = data[i]["user_statistics"].bit_time;
                                              region_count[item.city][cdn_temp]["center"] = [0, 0, 0, 0, 0];
                                              if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1) {
                                                region_count[item.city]["all"]["center"] += 1;
                                                region_count[item.city][cdn_temp]["center"][operator_locate[operator_temp]] = 1;
                                                region_count[item.city][cdn_temp]["center"][0] = 1;
                                              } else {
                                                region_count[item.city]["all"]["center"] += 0;
                                              }
                                              output[item.city][cdn_temp] = {};
                                              output[item.city][cdn_temp]["_bitrate"] = [data[i]["user_statistics"].bit_sum, -1, -1, -1, -1];
                                              output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] = data[i]["user_statistics"].bit_sum;
                                              output[item.city][cdn_temp]["_freeze_rate"] = [data[i]["user_statistics"].freeze_rate, -1, -1, -1, -1];
                                              output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] = data[i]["user_statistics"].freeze_rate;
                                              output[item.city][cdn_temp]["_band_width"] = data[i]["user_statistics"].band_width;
                                              output[item.city][cdn_temp]["_success_rate"] = [data[i]["user_statistics"].success_rate, -1, -1, -1, -1];
                                              output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] = data[i]["user_statistics"].success_rate;
                                            }
                                          }
                                        } else {
                                          // console.log("item");
                                          // console.log(item.city);
                                          region_count[item.city] = {};
                                          region_count[item.city]["all"] = {"sum":1,'bit_time':data[i]["user_statistics"].bit_time};
                                          //region_count[item.city]["all"]["sum"] = 1;
                                          region_count[item.city][cdn_temp] = {};
                                          //region_count[item.city][cdn_temp]["sum"] = [];
                                          region_count[item.city][cdn_temp]["center"] = [0, 0, 0, 0, 0];
                                          region_count[item.city][cdn_temp]["sum"] = [1, 0, 0, 0, 0];
                                          region_count[item.city][cdn_temp]["sum"][operator_locate[operator_temp]] = 1;
                                          region_count[item.city][cdn_temp]["bit_time"] = [data[i]["user_statistics"].bit_time, 0, 0, 0, 0];
                                          region_count[item.city][cdn_temp]["bit_time"][operator_locate[operator_temp]] = data[i]["user_statistics"].bit_time;
                                          if ((level_temp == 3 || level_temp == 4)&& vip_temp!=1) {
                                            region_count[item.city]["all"]["center"] = 1;
                                            region_count[item.city][cdn_temp]["center"][operator_locate[operator_temp]] = 1;
                                            region_count[item.city][cdn_temp]["center"][0] = 1;
                                          } else {
                                            region_count[item.city]["all"]["center"] = 0;
                                          }
                                          output[item.city] = {};
                                          output[item.city]["all"] = {};
                                          output[item.city]["all"]["_bitrate"] = data[i]["user_statistics"].bit_sum;
                                          output[item.city]["all"]["_freeze_rate"] = data[i]["user_statistics"].freeze_rate;
                                          output[item.city]["all"]["_band_width"] = data[i]["user_statistics"].band_width;
                                          output[item.city]["all"]["_success_rate"] = data[i]["user_statistics"].success_rate;
                                          output[item.city][cdn_temp] = {};
                                          output[item.city][cdn_temp]["_bitrate"] = [data[i]["user_statistics"].bit_sum, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_bitrate"][operator_locate[operator_temp]] = data[i]["user_statistics"].bit_sum;
                                          output[item.city][cdn_temp]["_freeze_rate"] = [data[i]["user_statistics"].freeze_rate, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_freeze_rate"][operator_locate[operator_temp]] = data[i]["user_statistics"].freeze_rate;
                                          output[item.city][cdn_temp]["_band_width"] = data[i]["user_statistics"].band_width;
                                          output[item.city][cdn_temp]["_success_rate"] = [data[i]["user_statistics"].success_rate, -1, -1, -1, -1];
                                          output[item.city][cdn_temp]["_success_rate"][operator_locate[operator_temp]] = data[i]["user_statistics"].success_rate;
                                        }
                                      }
                                }
                                //console.log(output['北京'])
                                for (var region_out in output) {
                                      for (var data_out in output[region_out]) {
                                        if (data_out == "all") {
                                          if (output[region_out][data_out]._bitrate > 0 ) {
                                            output[region_out][data_out]._bitrate = (output[region_out][data_out]._bitrate / (region_count[region_out][data_out].bit_time)).toFixed(2)
                                          }
                                          if (output[region_out][data_out]._freeze_rate > 0 && (region_count[region_out][data_out].sum - region_count[region_out][data_out].center) > 0) {
                                            output[region_out][data_out]._freeze_rate = (output[region_out][data_out]._freeze_rate / (region_count[region_out][data_out].sum - region_count[region_out][data_out].center)).toFixed(2)
                                          }
                                          if (output[region_out][data_out]._success_rate > 0) {
                                            output[region_out][data_out]._success_rate = (output[region_out][data_out]._success_rate / region_count[region_out][data_out].sum).toFixed(2)
                                          }
                                          output[region_out][data_out]._band_width = (output[region_out][data_out]._band_width - 0).toFixed(2)
                                        } else {
                                          for (var i = 0; i < 6; i++) {
                                            if (output[region_out][data_out]["_bitrate"][i] > 0 ) {
                                              output[region_out][data_out]["_bitrate"][i] = (output[region_out][data_out]["_bitrate"][i] / (region_count[region_out][data_out]["bit_time"][i])).toFixed(2)
                                            }
                                            if (output[region_out][data_out]["_freeze_rate"][i] > 0 && (region_count[region_out][data_out]["sum"][i] - region_count[region_out][data_out]["center"][i])!=0) {
                                              output[region_out][data_out]["_freeze_rate"][i] = (output[region_out][data_out]["_freeze_rate"][i] / (region_count[region_out][data_out]["sum"][i] - region_count[region_out][data_out]["center"][i])).toFixed(2)
                                            }
                                            if (output[region_out][data_out]["_success_rate"][i] > 0 ) {
                                              output[region_out][data_out]["_success_rate"][i] = (output[region_out][data_out]["_success_rate"][i] / region_count[region_out][data_out]["sum"][i]).toFixed(2)
                                            }
                                            output[region_out][data_out]["_band_width"] = (output[region_out][data_out]["_band_width"] - 0).toFixed(2)
                                          }
                                        }
                                      }
                                };
                                db.close()
                                callback(null,output);
                            } else {
                                db.close()
                                return res.json({
                                    ErrNo: "102",
                                    ErrMsg: "Failed to get logs"
                                });
                            }
                        })
                        delete params_region;

                    } else {
                        db.close()
                        res.json({
                            Err: err,
                            ErrNo: "101",
                            ErrMsg: "Failed to get table"
                          })
                        }
                        db.close();
                    });

              });
            // console.log(output);
            // callback(null,output);
        },
        function(err,results)
        {
            // console.log("huaqings"+results);
            res.json(results);
        });
    // }catch(e){
    //     res.json({
    //         ErrNo:"100",
    //         ErrMsg:"数据库错误"
    //     })
    // }
}




//var times=0;  
var quickSort=function(arr){   
    //如果数组长度小于等于1无需判断直接返回即可  
    if(arr.length<=1){  
        return arr;  
    }  
    var midIndex=Math.floor(arr.length/2);//取基准点  
    var midIndexVal=arr.splice(midIndex,1);//取基准点的值,splice(index,1)函数可以返回数组中被删除的那个数arr[index+1]  
    var left=[];//存放比基准点小的数组  
    var right=[];//存放比基准点大的数组  
    //遍历数组，进行判断分配  
    for(var i=0;i<arr.length;i++){  
        if(arr[i]<midIndexVal){  
            left.push(arr[i]);//比基准点小的放在左边数组  
        }  
        else{  
            right.push(arr[i]);//比基准点大的放在右边数组  
        }  
        //console.log("第"+(++times)+"次排序后："+arr);  
    }  
    //递归执行以上操作,对左右两个数组进行操作，直到数组长度为<=1；  
    return quickSort(left).concat(midIndexVal,quickSort(right));  
};  


var get_node_history_func = function(req,res){
    var output={};
    try{
        connect_mongo(res,function(db){
                db.collection('history_node',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        type=req.body.history_type;

                        // { liupan add, 2018/3/9
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != 'none') {
                            cdn_list = req.body.cdn_list.split(",");
                            query['cdn'] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/9

                        var back = {
                            "_id":0,
                            "time":1,
                            "region":1,
                            "operator":1,
                            "cdn":1
                        }
                        if (type == "jamnumperminute"){
                            back['jam_all'] = 1;
                            back['duration'] = 1;
                        }else{
                            back[type]=1;
                        }
                        //console.log(back);
                        logger.info(query);
                        logger.info(back);
                        var max_min_value_dic={'max':-999999,'min':9999999999};
                        var max_min_time_dic={'max':0,'min':0};
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};
                                var sum_jam = 0;
                                var sum_duration = 0;
                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    // { liupan add, 2018/6/12
                                    if (type=="jamnumperminute"){
                                        var jam_all = data_temp['jam_all']  ? parseInt(data_temp['jam_all']) : 0;
                                        var duration = data_temp['duration'] ? parseInt(data_temp['duration']) : 0;
                                        sum_jam += jam_all;
                                        sum_duration += duration;
                                        if (duration==0){
                                            data_value = 0
                                        }else {
                                            data_value = jam_all / (duration /1000 /60)}
                                    }
                                    else{
                                        if (!(type in data_temp)) continue;
                                        data_value = data_temp[type];
                                    }
                                        // } liupan add, 2018/6/12
                                    if (output[data_temp["time"]])
                                    {
                                        output[data_temp["time"]]+=data_value;
                                        out_count[data_temp["time"]]+=1;
                                    }
                                    else
                                    {
                                        output[data_temp["time"]]=data_value;
                                        out_count[data_temp["time"]]=1;

                                        // if(data_temp[type]!=0)
                                        // {
                                        //     out_count[data_temp["time"]]=1;
                                        // }
                                        // else
                                        // {
                                        //     out_count[data_temp["time"]]=0;
                                        // }
                                    }

                                }
                                var band_arr=[];
                                // { liupan add, 2017/12/12
                                var band_4thDayPeak = {};   // 用于第四日峰值带宽统计方法：记录每一天的最大值
                                var band_4thDayPeak_arr = [];
                                // } liupan add, 2017/12/12
                                if(type=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        band_arr.push(output[t]);
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        else if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }
                                        // { liupan add, 2017/12/12
                                        var date = new Date(t * 1000);
                                        var ymd = "" + date.getFullYear() + date.getMonth() + date.getDate();
                                        if (!(ymd in band_4thDayPeak)) {
                                            band_4thDayPeak[ymd] = output[t];
                                        }
                                        else {
                                            if (band_4thDayPeak[ymd] < output[t])
                                                band_4thDayPeak[ymd] = output[t];
                                        }
                                        // } liupan add, 2017/12/12
                                    }

                                    // { liupan add, 2017/12/12
                                    for (var ymd in band_4thDayPeak) {
                                        band_4thDayPeak_arr.push(band_4thDayPeak[ymd]);
                                    }
                                    // } liupan add, 2017/12/12
                                }
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                        {
                                            // { liupan add, 2018/6/12
                                            if (type != 'user_n' && type != 'req_n') 
                                            // } liupan add, 2018/6/12
                                            output[t]=output[t]/out_count[t];
                                        }
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        else if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }
                                    }
                                }
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                for(var tt in output)
                                {
                                    ori_count+=1;
                                    if(value_count==0)
                                    {
                                        value_first_time=tt;
                                    }
                                    value_count+=1;
                                    value_sum+=output[tt];
                                    if(value_count >=LIMIT_COUNT)
                                    {
                                        out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                        test_count+=1;
                                        value_sum=0;
                                        value_count=0;
                                    }
                                }
                                out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        // { liupan add, 2017/12/12
                                        var sort_b4dp = quickSort(band_4thDayPeak_arr);
                                        var band4dp = 0;
                                        if (sort_b4dp.length >= 4) {
                                            band4dp = sort_b4dp[sort_b4dp.length - 4];
                                        }
                                        else {
                                            band4dp = sort_b4dp[0];
                                        }
                                        // } liupan add, 2017/12/12
                                        all_output["detail"]=out_result;
                                        // { liupan modify, 2017/12/12
                                        // all_output["band95"]=band95;
                                        if (config.bandwidth_measurement_method == 0) {
                                            all_output["bandwidth95"]=band95;
                                        }
                                        else if (config.bandwidth_measurement_method == 1) {
                                            all_output["bandwidth4dp"] = band4dp;
                                        }
                                        // } liupan modify, 2017/12/12
                                        break;
                                    case "jamnumperminute":
                                        all_output["detail"]=out_result;
                                        if(sum_duration==0){
                                            all_output["jamnumAverage"]=0;
                                        }else {
                                            all_output["jamnumAverage"] = sum_jam / (sum_duration / 1000 / 60);
                                        }
                                    // { liupan modify, 2018/6/12
                                    // case "bitrate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "freeze_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "success_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "user_n":
                                    case "req_n":
                                    case "freeze_avg_iv":
                                    case "delayed_avg":
                                        all_output["detail"]=out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/12
                                }
                                //console.log(output);
                                res.json(all_output);
                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
var get_node_sum_history_func = function(req,res){
    var output={};
    try{
        connect_mongo(res,function(db){
                db.collection('history_node_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        type=req.body.history_type;
                        // { liupan add, 2018/3/8
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                        }
                        // } liupan add, 2018/3/8

                        var back = {
                            "_id":0,
                            "time":1,
                            //"region":1,
                            //"operator":1,
                            "cdn":1
                        };
                        if (type == "jamnumperminute"){
                            back['jam_all'] = 1;
                            back['duration'] = 1;
                        }else {
                            back[type]=1;
                        }
                        //console.log(back);
                        var max_min_value_dic={'max':-999999,'min':9999999999};
                        var max_min_time_dic={'max':0,'min':0};
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};
                                var sum_jam = 0;
                                var sum_duration = 0;
                                for (var i=0; i <logs.length;i++)
                                {
                                    if (type=="jamnumperminute"&&cdn_temp==0){
                                        var data_temp=logs[i];
                                        var data_time_stamp=data_temp["time"];
                                        var jam_all = data_temp['jam_all']  ? parseInt(data_temp['jam_all']) : 0;
                                        var duration = data_temp['duration'] ? parseInt(data_temp['duration']) : 0;
                                        sum_jam += jam_all;
                                        sum_duration += duration;
                                        if (duration==0){
                                            data_value = 0
                                        }else {
                                            data_value = jam_all / (duration /1000 /60);
                                        };
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        else if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        if (output[data_time_stamp]) //累加
                                        {
                                            output[data_time_stamp]+=data_value;
                                            out_count[data_time_stamp]+=1;
                                        }
                                        else
                                        {
                                            output[data_time_stamp]=data_value;
                                            out_count[data_time_stamp]=1;
                                        }



                                    }else{
                                        var data_temp=logs[i];
                                        // { liupan add, 2018/6/12
                                        if (!(type in data_temp)) continue;
                                        // } liupan add, 2018/6/12
                                        var data_time_stamp=data_temp["time"];
                                        // { liupan modify, 2018/3/8
                                        // var data_value=data_temp[type]
                                        var data_value = 0;
                                        if (cdn_list.length == 0) {
                                            data_value = data_temp[type];
                                        }
                                        else {
                                            var cdn_val_count = 0;
                                            for (var j = 0; j < cdn_list.length; ++j) {
                                                if (cdn_list[j] in data_temp['cdn']) {
                                                    data_value += data_temp['cdn'][cdn_list[j]][type];

                                                    if (data_temp['cdn'][cdn_list[j]]["band_width"] > 0)
                                                        cdn_val_count++;
                                                }
                                            }

                                            if (type == "success_rate" || type == "freeze_rate" || type == "bitrate") {
                                                if (cdn_val_count > 0)
                                                    data_value /= cdn_val_count;
                                            }
                                        }
                                        // } liupan modify, 2018/3/8
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        else if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        if (output[data_time_stamp]) //累加
                                        {
                                            if(data_value!=0)
                                            {
                                                output[data_time_stamp]+=data_value;
                                                out_count[data_time_stamp]+=1;
                                            }
                                        }
                                        else
                                        {
                                            output[data_time_stamp]=data_value;
                                            if(data_value!=0)
                                            {
                                                out_count[data_time_stamp]=1;
                                            }
                                            else
                                            {
                                                out_count[data_time_stamp]=0;
                                            }
                                        }
                                    }

                                }


                                //求各时间点数据的平均值
                                var band_arr=[];
                                // { liupan add, 2017/12/12
                                var band_4thDayPeak = {};   // 用于第四日峰值带宽统计方法：记录每一天的最大值
                                var band_4thDayPeak_arr = [];
                                // } liupan add, 2017/12/12
                                if(type=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        band_arr.push(output[t]);
                                        // { liupan add, 2017/12/12
                                        var date = new Date(t * 1000);
                                        var ymd = "" + date.getFullYear() + date.getMonth() + date.getDate();
                                        if (!(ymd in band_4thDayPeak)) {
                                            band_4thDayPeak[ymd] = output[t];
                                        }
                                        else {
                                            if (band_4thDayPeak[ymd] < output[t])
                                                band_4thDayPeak[ymd] = output[t];
                                        }
                                        // } liupan add, 2017/12/12
                                    }

                                    // { liupan add, 2017/12/12
                                    for (var ymd in band_4thDayPeak) {
                                        band_4thDayPeak_arr.push(band_4thDayPeak[ymd]);
                                    }
                                    // } liupan add, 2017/12/12
                                }
                                // { liupan add, 2018/6/12
                                else if (type == 'user_n' || type == 'req_n') {
                                    // do nothing
                                }
                                // } liupan add, 2018/6/12
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                            output[t]=output[t]/out_count[t];
                                    }
                                }
                                
                                //LIMIT_COUNT 个时间点求平均数
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};   //各时间点数据统计
                                var test_count=0;
                                var ori_count=0;
                                for(var tt in output)
                                {
                                    ori_count+=1;
                                    if(value_count==0)
                                    {
                                        value_first_time=tt;
                                    }
                                    value_count+=1;
                                    value_sum+=output[tt];
                                    if(value_count >=LIMIT_COUNT)
                                    {
                                        out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                        test_count+=1;
                                        value_sum=0;
                                        value_count=0;
                                    }
                                }


                                if(test_count == 0)
                                {
                                    out_result[max_min_time_dic['max']]=0;
                                    out_result[max_min_time_dic['min']]=0;
                                }
                                else
                                {
                                    out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                    out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                }
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        // { liupan add, 2017/12/12
                                        var sort_b4dp = quickSort(band_4thDayPeak_arr);
                                        var band4dp = 0;
                                        if (sort_b4dp.length >= 4) {
                                            band4dp = sort_b4dp[sort_b4dp.length - 4];
                                        }
                                        else {
                                            band4dp = sort_b4dp[0];
                                        }
                                        // } liupan add, 2017/12/12
                                        all_output["detail"]=out_result;
                                        // { liupan modify, 2017/12/12
                                        // all_output["band95"]=band95;
                                        if (config.bandwidth_measurement_method == 0) {
                                            all_output["bandwidth95"]=band95;
                                        }
                                        else if (config.bandwidth_measurement_method == 1) {
                                            all_output["bandwidth4dp"] = band4dp;
                                        }
                                        // } liupan modify, 2017/12/12
                                        break;

                                    case "jamnumperminute":
                                        all_output["detail"]=out_result;
                                        if (sum_duration==0){
                                            all_output["jamnumAverage"] = 0;
                                        }else {
                                            all_output["jamnumAverage"] = sum_jam / (sum_duration / 1000 / 60);
                                        }
                                    // { liupan modify, 2018/6/12
                                    // case "bitrate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "freeze_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "success_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "user_n":
                                    case "req_n":
                                    case "freeze_avg_iv":
                                    case "delayed_avg":
                                        all_output["detail"] = out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/12
                                }
                                res.json(all_output);
                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

// { liupan add, 2017/8/1
var get_node_sum_cdn_history_common_func = function(req,res){
    var output={};
    var kw_output={};
    var ws_output={};
    var dl_output={};
    console.log(cdn_detail);
    try{
        connect_mongo(res,function(db){
                db.collection('history_node_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=0;//req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        //type=req.body.history_type;

                        // { liupan add, 2018/3/8
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != 'none') {
                            cdn_list = req.body.cdn_list.split(",");
                        }
                        // } liupan add, 2018/3/8

                        var back = {
                            "_id":0,
                            "time":1,
                            "band_width":1,
                            "edge_band_width":1,
                            // { liupan add, 2018/2/12
                            "h5_band_width":1,
                            // } liupan add, 2018/2/12
                            "freeze_rate":1,
                            "bitrate":1,
                            "success_rate":1,
                            //"region":1,
                            //"operator":1,
                            "cdn":1
                        }
                        //back[type]=1;
                        //console.log(back);
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }

                                var out_count = 0;
                                var time_cdn_output = {};//time_cdn_output[time][cdn][type] = value;
                                var final_time_cdn_output = {};
                                var cdn_all = ["all"];
                                // { liupan modify, 2018/3/8
                                // for (var i = 0; i < cdn_detail.length; ++i) {
                                //     cdn_all[i + 1] = cdn_detail[i];
                                // }
                                if (cdn_list.length == 0) {
                                    for (var i = 0; i < cdn_detail.length; ++i) {
                                        cdn_all[i + 1] = cdn_detail[i];
                                    }
                                }
                                else {
                                    for (var i = 0; i < cdn_list.length; ++i) {
                                        cdn_all[i + 1] = cdn_list[i];
                                    }
                                }
                                // } liupan modify, 2018/3/8
                                // { liupan modify, 2018/2/12
                                // var type_all = ["band_width","edge_band_width","freeze_rate","bitrate","success_rate"];
                                var type_all = ["band_width","edge_band_width","freeze_rate","bitrate","success_rate","h5_band_width"];
                                // } liupan modify, 2018/2/12
                                var max_bw_time = {};
                                var max_bw_value = {};
                                var min_bw_time = {};
                                var min_bw_value = {};
                                for(var i=0;i<cdn_all.length;i++)
                                {
                                    max_bw_time[cdn_all[i]] = 0;
                                    // { liupan modify, 2017/10/1
                                    // max_bw_value[cdn_all[i]] = 0;
                                    max_bw_value[cdn_all[i]] = -1;
                                    // } liupan modify, 2017/10/1
                                    min_bw_time[cdn_all[i]] = 0;
                                    min_bw_value[cdn_all[i]] = 9999999999;
                                }
                                //取logs中所有数据
                                for (var i=0; i <logs.length;i++)
                                {
                                    out_count++;
                                    var data_temp=logs[i];
                                    var data_time_stamp=data_temp["time"];
                                    time_cdn_output[data_time_stamp] = {};
                                    for(var j=0;j<cdn_all.length;j++)
                                    {
                                        time_cdn_output[data_time_stamp][cdn_all[j]] = {};
                                    }
                                    for(var type_i=0;type_i<type_all.length;type_i++)
                                    {
                                        var type = type_all[type_i];
                                        var all_value=data_temp[type];
                                        // { liupan modify, 2018/2/12
                                        // if(all_value == null) all_value = 0;
                                        if(all_value == null || all_value == undefined) all_value = 0;
                                        // } liupan modify, 2018/2/12
                                        
                                        // { liupan add, 2018/3/8
                                        if (cdn_list.length > 0) {
                                            all_value = 0;
                                        }
                                        var valid_count = 0;
                                        // } liupan add, 2018/3/8

                                        var cdn_value = {};
                                        for (var j =1; j < cdn_all.length; ++j) {
                                            cdn_value[cdn_all[j]] = 0;
                                            if (data_temp["cdn"].hasOwnProperty(cdn_all[j])) {
                                                cdn_value[cdn_all[j]] = data_temp['cdn'][cdn_all[j]][type];
                                                // { liupan modify, 2018/2/12
                                                // if (cdn_value[cdn_all[j]] == null) {
                                                if (cdn_value[cdn_all[j]] == null || cdn_value[cdn_all[j]] == undefined) {
                                                // } liupan modify, 2018/2/12
                                                    cdn_value[cdn_all[j]] = 0;
                                                }
                                                // { liupan add, 2018/3/8
                                                if (data_temp['cdn'][cdn_all[j]]["band_width"] > 0) {
                                                    valid_count++;
                                                }
                                                // } liupan add, 2018/3/8
                                            }

                                            // { liupan add, 2018/3/8
                                            if (cdn_list.length > 0) {
                                                all_value += cdn_value[cdn_all[j]];
                                            }
                                            // } liupan add, 2018/3/8
                                        }

                                        // { liupan add, 2018/3/8
                                        if (cdn_list.length > 0 && valid_count > 0 && 
                                            (type == "success_rate" || type == "freeze_rate" || type == "bitrate")) {
                                            all_value /= valid_count;
                                        }
                                        // } liupan add, 2018/3/8
                                        
                                        if(type == 'band_width')
                                        {
                                            for(var k=0;k<cdn_all.length;k++)
                                            {
                                                var tmp_value = 0;
                                                var tmp_cdn = cdn_all[k];
                                                if (tmp_cdn == "all") {
                                                    tmp_value = all_value;
                                                }
                                                else {
                                                    tmp_value = cdn_value[tmp_cdn];
                                                }
                                                // { liupan delete, 2017/10/1
                                                // if(tmp_value <= 0)
                                                //     continue;
                                                // } liupan delete, 2017/10/1
                                                if(max_bw_value[tmp_cdn] < tmp_value)
                                                {
                                                    max_bw_time[tmp_cdn] = data_time_stamp;
                                                    max_bw_value[tmp_cdn] = tmp_value;
                                                }
                                                if(min_bw_value[tmp_cdn] > tmp_value)
                                                {
                                                    min_bw_time[tmp_cdn] = data_time_stamp;
                                                    min_bw_value[tmp_cdn] = tmp_value;
                                                }
                                            }
                                        }
                                        time_cdn_output[data_time_stamp]['all'][type] = all_value;
                                        for (var j = 1; j < cdn_all.length; ++j) {
                                            time_cdn_output[data_time_stamp][cdn_all[j]][type] = cdn_value[cdn_all[j]];
                                        }
                                    }
                                }
                                var tmp_cdn_type_value = {};
                                var value_count=0;
                                for(var i=0;i<cdn_all.length;i++)
                                {
                                    var cdn_tmp = cdn_all[i];
                                    tmp_cdn_type_value[cdn_tmp] = {};
                                    for(var j=0;j<type_all.length;j++)
                                    {
                                        var type_tmp = type_all[j];
                                        tmp_cdn_type_value[cdn_tmp][type_tmp] = 0;
                                    }
                                }

                                //对取出的数据进行数据量上的处理
                                var node_history_start_time = Number(req.body.start);
                                var node_history_end_time = Number(req.body.end);
                                var do_be = 5;
                                var do_TIME_INTERVAL_MAX = 0;
                                if(do_be <= 1)
                                {
                                    do_TIME_INTERVAL_MAX = TIME_INTERVAL_MAX;
                                }
                                else
                                {
                                    do_TIME_INTERVAL_MAX = TIME_INTERVAL_MAX/do_be;
                                }

                                LIMIT_COUNT=parseInt((node_history_end_time-node_history_start_time)/do_TIME_INTERVAL_MAX)+1;
                                var time_last_tmp = 0;
                                for(var time_i in time_cdn_output)
                                {
                                    var data_tmp = time_cdn_output[time_i];
                                    time_last_tmp = time_i;
                                    // { liupan modify, 2017/10/1
                                    // var flag=0;
                                    var flag = 1;
                                    // } liupan modify, 2017/10/1
                                    for(var i=0;i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        // { liupan modify, 2017/10/1
                                        // if(data_tmp[cdn_tmp]['band_width']==0)
                                        // {
                                        //     flag=1;
                                        //     break;
                                        // }
                                        if (data_tmp[cdn_tmp]['band_width'] > 0) {
                                            flag = 0;
                                            break;
                                        }
                                        // } liupan modify, 2017/10/1
                                    }
                                    for(var i=0;flag==0 && i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        for(var j=0;j<type_all.length;j++)
                                        {
                                            
                                            var type_tmp = type_all[j];
                                            tmp_cdn_type_value[cdn_tmp][type_tmp] += data_tmp[cdn_tmp][type_tmp];
                                        }
                                    }
                                    if(flag==1)
                                    {
                                        continue;
                                    }
                                    value_count++;
                                    if(value_count >= LIMIT_COUNT)
                                    {
                                        value_count = 0;
                                        if(!final_time_cdn_output.hasOwnProperty(time_i))
                                            final_time_cdn_output[time_i] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_i][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                if(0)
                                                {
                                                    final_time_cdn_output[time_i][cdn_tmp][type_tmp] = tmp_cdn_type_value[cdn_tmp][type_tmp];
                                                }
                                                else
                                                {
                                                    final_time_cdn_output[time_i][cdn_tmp][type_tmp] = tmp_cdn_type_value[cdn_tmp][type_tmp]/LIMIT_COUNT;
                                                }
                                                tmp_cdn_type_value[cdn_tmp][type_tmp] = 0;
                                            }
                                        }
                                    }
                                }

                                //加入最后一个值
                                if(!final_time_cdn_output.hasOwnProperty(time_last_tmp) && time_cdn_output.hasOwnProperty(time_last_tmp))
                                {
                                    final_time_cdn_output[time_last_tmp] = {};
                                    for(var i=0;i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        final_time_cdn_output[time_last_tmp][cdn_tmp] = {};
                                        for(var j=0;j<type_all.length;j++)
                                        {
                                            var type_tmp = type_all[j];
                                            final_time_cdn_output[time_last_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_last_tmp][cdn_tmp][type_tmp];
                                        }
                                    }
                                }

                                //加入极值
                                for(var k=0;k<cdn_all.length;k++)
                                {
                                    var time_tmp = max_bw_time[cdn_all[k]];
                                    if(!final_time_cdn_output.hasOwnProperty(time_tmp))
                                    {
                                        final_time_cdn_output[time_tmp] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_tmp][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                final_time_cdn_output[time_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_tmp][cdn_tmp][type_tmp];
                                            }
                                        }
                                    }
                                    var time_tmp = min_bw_time[cdn_all[k]];
                                    if(!final_time_cdn_output.hasOwnProperty(time_tmp))
                                    {
                                        final_time_cdn_output[time_tmp] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_tmp][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                final_time_cdn_output[time_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_tmp][cdn_tmp][type_tmp];
                                            }
                                        }
                                    }
                                }
                                
                                // { liupan modify, 2018/6/7
                                // res.json(final_time_cdn_output);
                                if (req.body.cdn_list == '31_link' || req.body.cdn_list == '首都在线') {
                                    // 日本管控由于数据丢失，只恢复了每日峰值数据，所以客户查询时，此处统一只返回每日峰值数据。
                                    var final_time_cdn_output_f = {};

                                    var max_value = {};
                                    var max_value_time = {};
                                    for (var t in time_cdn_output) {
                                        var tmpDate = new Date(t * 1000);
                                        var time = "" + tmpDate.getFullYear() + "-" + tmpDate.getMonth() + "-" + tmpDate.getDate() + " 08:00:00";
                                        if (time in max_value_time) {
                                            if (max_value[time] < time_cdn_output[t][req.body.cdn_list]["band_width"]) {
                                                max_value[time] = time_cdn_output[t][req.body.cdn_list]["band_width"];
                                                max_value_time[time] = t;
                                            }
                                        }
                                        else {
                                            max_value[time] = time_cdn_output[t][req.body.cdn_list]["band_width"];
                                            max_value_time[time] = t;
                                        }
                                    }

                                    for (var t in max_value_time) {
                                        var ctime = max_value_time[t];
                                        final_time_cdn_output_f[ctime] = {"all":time_cdn_output[ctime]["all"]};
                                        final_time_cdn_output_f[ctime][req.body.cdn_list] = time_cdn_output[ctime][req.body.cdn_list];
                                    }

                                    res.json(final_time_cdn_output_f);
                                }
                                else {
                                    res.json(final_time_cdn_output);
                                }
                                // } liupan modify, 2018/6/7
                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
// } liupan add, 2017/8/1

var get_node_sum_cdn_history_func = function(req,res){
    var output={};
    var kw_output={};
    var ws_output={};
    var dl_output={};
    try{
        connect_mongo(res,function(db){
                db.collection('history_node_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=0;//req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        //type=req.body.history_type;

                        var back = {
                            "_id":0,
                            "time":1,
                            "band_width":1,
                            "edge_band_width":1,
                            "freeze_rate":1,
                            "bitrate":1,
                            "success_rate":1,
                            //"region":1,
                            //"operator":1,
                            "cdn":1
                        }
                        //back[type]=1;
                        //console.log(back);
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }

                                var out_count = 0;
                                var time_cdn_output = {};//time_cdn_output[time][cdn][type] = value;
                                var final_time_cdn_output = {};
                                var cdn_all = ["all","kw","ws","dl"];
                                var type_all = ["band_width","edge_band_width","freeze_rate","bitrate","success_rate"];
                                var max_bw_time = {};
                                var max_bw_value = {};
                                var min_bw_time = {};
                                var min_bw_value = {};
                                for(var i=0;i<cdn_all.length;i++)
                                {
                                    max_bw_time[cdn_all[i]] = 0;
                                    max_bw_value[cdn_all[i]] = 0;
                                    min_bw_time[cdn_all[i]] = 0;
                                    min_bw_value[cdn_all[i]] = 9999999999;
                                }
                                //取logs中所有数据
                                for (var i=0; i <logs.length;i++)
                                {
                                    out_count++;
                                    var data_temp=logs[i];
                                    var data_time_stamp=data_temp["time"];
                                    time_cdn_output[data_time_stamp] = {};
                                    for(var j=0;j<cdn_all.length;j++)
                                    {
                                        time_cdn_output[data_time_stamp][cdn_all[j]] = {};
                                    }
                                    for(var type_i=0;type_i<type_all.length;type_i++)
                                    {
                                        var type = type_all[type_i];
                                        var all_value=data_temp[type];
                                        if(all_value == null) all_value = 0;
                                        
                                        var kw_value=0;
                                        var ws_value=0;
                                        var dl_value=0;
                                        if(data_temp["cdn"].hasOwnProperty("kw"))
                                        {
                                            kw_value=data_temp['cdn']['kw'][type];
                                            if(kw_value == null) kw_value = 0;
                                        }
                                        if(data_temp["cdn"].hasOwnProperty("ws"))
                                        {
                                            ws_value=data_temp['cdn']['ws'][type];
                                            if(ws_value == null) ws_value = 0;
                                        }    
                                        if(data_temp["cdn"].hasOwnProperty("dl"))
                                        {
                                            dl_value=data_temp['cdn']['dl'][type];
                                            if(dl_value == null) dl_value = 0;
                                        }

                                        if(type == 'band_width')
                                        {
                                            for(var k=0;k<cdn_all.length;k++)
                                            {
                                                var tmp_value = 0;
                                                var tmp_cdn = cdn_all[k];
                                                if(tmp_cdn == 'all')
                                                    tmp_value = all_value;
                                                else if(tmp_cdn == 'kw')
                                                    tmp_value = kw_value;
                                                else if(tmp_cdn == 'ws')
                                                    tmp_value = ws_value;
                                                else if(tmp_cdn == 'dl')
                                                    tmp_value = dl_value;
                                                if(tmp_value <= 0)
                                                    continue;
                                                if(max_bw_value[tmp_cdn] < tmp_value)
                                                {
                                                    max_bw_time[tmp_cdn] = data_time_stamp;
                                                    max_bw_value[tmp_cdn] = tmp_value;
                                                }
                                                if(min_bw_value[tmp_cdn] > tmp_value)
                                                {
                                                    min_bw_time[tmp_cdn] = data_time_stamp;
                                                    min_bw_value[tmp_cdn] = tmp_value;
                                                }
                                            }
                                        }
                                        time_cdn_output[data_time_stamp]['all'][type] = all_value;
                                        time_cdn_output[data_time_stamp]['kw'][type] = kw_value;
                                        time_cdn_output[data_time_stamp]['ws'][type] = ws_value;
                                        time_cdn_output[data_time_stamp]['dl'][type] = dl_value;
                                    }
                                }
                                var tmp_cdn_type_value = {};
                                var value_count=0;
                                for(var i=0;i<cdn_all.length;i++)
                                {
                                    var cdn_tmp = cdn_all[i];
                                    tmp_cdn_type_value[cdn_tmp] = {};
                                    for(var j=0;j<type_all.length;j++)
                                    {
                                        var type_tmp = type_all[j];
                                        tmp_cdn_type_value[cdn_tmp][type_tmp] = 0;
                                    }
                                }
                                //对取出的数据进行数据量上的处理
                                var node_history_start_time = Number(req.body.start);
                                var node_history_end_time = Number(req.body.end);
                                var do_be = 5;
                                var do_TIME_INTERVAL_MAX = 0;
                                if(do_be <= 1)
                                {
                                    do_TIME_INTERVAL_MAX = TIME_INTERVAL_MAX;
                                }
                                else
                                {
                                    do_TIME_INTERVAL_MAX = TIME_INTERVAL_MAX/do_be;
                                }
                                LIMIT_COUNT=parseInt((node_history_end_time-node_history_start_time)/do_TIME_INTERVAL_MAX)+1;
                                var time_last_tmp = 0;
                                for(var time_i in time_cdn_output)
                                {
                                    var data_tmp = time_cdn_output[time_i];
                                    time_last_tmp = time_i;
                                    var flag=0;
                                    for(var i=0;i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        if(data_tmp[cdn_tmp]['band_width']==0)
                                        {
                                            flag=1;
                                            break;
                                        }
                                    }
                                    for(var i=0;flag==0 && i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        for(var j=0;j<type_all.length;j++)
                                        {
                                            
                                            var type_tmp = type_all[j];
                                            tmp_cdn_type_value[cdn_tmp][type_tmp] += data_tmp[cdn_tmp][type_tmp];
                                        }
                                    }
                                    if(flag==1)
                                    {
                                        continue;
                                    }
                                    value_count++;
                                    if(value_count >= LIMIT_COUNT)
                                    {
                                        value_count = 0;
                                        if(!final_time_cdn_output.hasOwnProperty(time_i))
                                            final_time_cdn_output[time_i] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_i][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                if(0)
                                                {
                                                    final_time_cdn_output[time_i][cdn_tmp][type_tmp] = tmp_cdn_type_value[cdn_tmp][type_tmp];
                                                }
                                                else
                                                {
                                                    final_time_cdn_output[time_i][cdn_tmp][type_tmp] = tmp_cdn_type_value[cdn_tmp][type_tmp]/LIMIT_COUNT;
                                                }
                                                tmp_cdn_type_value[cdn_tmp][type_tmp] = 0;
                                            }
                                        }
                                    }
                                }
                                //加入最后一个值
                                if(!final_time_cdn_output.hasOwnProperty(time_last_tmp) && time_cdn_output.hasOwnProperty(time_last_tmp))
                                {
                                    final_time_cdn_output[time_last_tmp] = {};
                                    for(var i=0;i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        final_time_cdn_output[time_last_tmp][cdn_tmp] = {};
                                        for(var j=0;j<type_all.length;j++)
                                        {
                                            var type_tmp = type_all[j];
                                            final_time_cdn_output[time_last_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_last_tmp][cdn_tmp][type_tmp];
                                        }
                                    }
                                }
                                //加入极值
                                for(var k=0;k<cdn_all.length;k++)
                                {
                                    var time_tmp = max_bw_time[cdn_all[k]];
                                    if(!final_time_cdn_output.hasOwnProperty(time_tmp))
                                    {
                                        final_time_cdn_output[time_tmp] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_tmp][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                final_time_cdn_output[time_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_tmp][cdn_tmp][type_tmp];
                                            }
                                        }
                                    }
                                    var time_tmp = min_bw_time[cdn_all[k]];
                                    if(!final_time_cdn_output.hasOwnProperty(time_tmp))
                                    {
                                        final_time_cdn_output[time_tmp] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_tmp][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                final_time_cdn_output[time_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_tmp][cdn_tmp][type_tmp];
                                            }
                                        }
                                    }
                                }
                                
                                res.json(final_time_cdn_output);
                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

// { liupan add, 2017/7/20
var get_node_sum_jp_history_func = function(req,res){
    var output={};
    var kw_output={};
    var ws_output={};
    var dl_output={};
    try{
        connect_mongo(res,function(db){
                db.collection('history_node_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=0;//req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        //type=req.body.history_type;

                        var back = {
                            "_id":0,
                            "time":1,
                            "band_width":1,
                            "edge_band_width":1,
                            "freeze_rate":1,
                            "bitrate":1,
                            "success_rate":1,
                            //"region":1,
                            //"operator":1,
                            "cdn":1
                        }
                        //back[type]=1;
                        //console.log(back);
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }

                                var out_count = 0;
                                var time_cdn_output = {};//time_cdn_output[time][cdn][type] = value;
                                var final_time_cdn_output = {};
                                var cdn_all = ["all","sdzx"];
                                var type_all = ["band_width","edge_band_width","freeze_rate","bitrate","success_rate"];
                                var max_bw_time = {};
                                var max_bw_value = {};
                                var min_bw_time = {};
                                var min_bw_value = {};
                                for(var i=0;i<cdn_all.length;i++)
                                {
                                    max_bw_time[cdn_all[i]] = 0;
                                    max_bw_value[cdn_all[i]] = 0;
                                    min_bw_time[cdn_all[i]] = 0;
                                    min_bw_value[cdn_all[i]] = 9999999999;
                                }
                                //取logs中所有数据
                                for (var i=0; i <logs.length;i++)
                                {
                                    out_count++;
                                    var data_temp=logs[i];
                                    var data_time_stamp=data_temp["time"];
                                    time_cdn_output[data_time_stamp] = {};
                                    for(var j=0;j<cdn_all.length;j++)
                                    {
                                        time_cdn_output[data_time_stamp][cdn_all[j]] = {};
                                    }
                                    for(var type_i=0;type_i<type_all.length;type_i++)
                                    {
                                        var type = type_all[type_i];
                                        var all_value=data_temp[type];
                                        if(all_value == null) all_value = 0;
                                        
                                        var sdzx_value = 0;
                                        if(data_temp["cdn"].hasOwnProperty("sdzx"))
                                        {
                                            sdzx_value=data_temp['cdn']['sdzx'][type];
                                            if(sdzx_value == null) sdzx_value = 0;
                                        }

                                        if(type == 'band_width')
                                        {
                                            for(var k=0;k<cdn_all.length;k++)
                                            {
                                                var tmp_value = 0;
                                                var tmp_cdn = cdn_all[k];
                                                if(tmp_cdn == 'all')
                                                    tmp_value = all_value;
                                                else if(tmp_cdn == 'sdzx')
                                                    tmp_value = sdzx_value;
                                                if(tmp_value <= 0)
                                                    continue;
                                                if(max_bw_value[tmp_cdn] < tmp_value)
                                                {
                                                    max_bw_time[tmp_cdn] = data_time_stamp;
                                                    max_bw_value[tmp_cdn] = tmp_value;
                                                }
                                                if(min_bw_value[tmp_cdn] > tmp_value)
                                                {
                                                    min_bw_time[tmp_cdn] = data_time_stamp;
                                                    min_bw_value[tmp_cdn] = tmp_value;
                                                }
                                            }
                                        }
                                        time_cdn_output[data_time_stamp]['all'][type] = all_value;
                                        time_cdn_output[data_time_stamp]['sdzx'][type] = sdzx_value;
                                    }
                                }
                                var tmp_cdn_type_value = {};
                                var value_count=0;
                                for(var i=0;i<cdn_all.length;i++)
                                {
                                    var cdn_tmp = cdn_all[i];
                                    tmp_cdn_type_value[cdn_tmp] = {};
                                    for(var j=0;j<type_all.length;j++)
                                    {
                                        var type_tmp = type_all[j];
                                        tmp_cdn_type_value[cdn_tmp][type_tmp] = 0;
                                    }
                                }
                                //对取出的数据进行数据量上的处理
                                var node_history_start_time = Number(req.body.start);
                                var node_history_end_time = Number(req.body.end);
                                var do_be = 5;
                                var do_TIME_INTERVAL_MAX = 0;
                                if(do_be <= 1)
                                {
                                    do_TIME_INTERVAL_MAX = TIME_INTERVAL_MAX;
                                }
                                else
                                {
                                    do_TIME_INTERVAL_MAX = TIME_INTERVAL_MAX/do_be;
                                }
                                LIMIT_COUNT=parseInt((node_history_end_time-node_history_start_time)/do_TIME_INTERVAL_MAX)+1;
                                var time_last_tmp = 0;
                                for(var time_i in time_cdn_output)
                                {
                                    var data_tmp = time_cdn_output[time_i];
                                    time_last_tmp = time_i;
                                    var flag=0;
                                    for(var i=0;i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        if(data_tmp[cdn_tmp]['band_width']==0)
                                        {
                                            flag=1;
                                            break;
                                        }
                                    }
                                    for(var i=0;flag==0 && i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        for(var j=0;j<type_all.length;j++)
                                        {
                                            
                                            var type_tmp = type_all[j];
                                            tmp_cdn_type_value[cdn_tmp][type_tmp] += data_tmp[cdn_tmp][type_tmp];
                                        }
                                    }
                                    if(flag==1)
                                    {
                                        continue;
                                    }
                                    value_count++;
                                    if(value_count >= LIMIT_COUNT)
                                    {
                                        value_count = 0;
                                        if(!final_time_cdn_output.hasOwnProperty(time_i))
                                            final_time_cdn_output[time_i] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_i][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                if(0)
                                                {
                                                    final_time_cdn_output[time_i][cdn_tmp][type_tmp] = tmp_cdn_type_value[cdn_tmp][type_tmp];
                                                }
                                                else
                                                {
                                                    final_time_cdn_output[time_i][cdn_tmp][type_tmp] = tmp_cdn_type_value[cdn_tmp][type_tmp]/LIMIT_COUNT;
                                                }
                                                tmp_cdn_type_value[cdn_tmp][type_tmp] = 0;
                                            }
                                        }
                                    }
                                }
                                //加入最后一个值
                                if(!final_time_cdn_output.hasOwnProperty(time_last_tmp) && time_cdn_output.hasOwnProperty(time_last_tmp))
                                {
                                    final_time_cdn_output[time_last_tmp] = {};
                                    for(var i=0;i<cdn_all.length;i++)
                                    {
                                        var cdn_tmp = cdn_all[i];
                                        final_time_cdn_output[time_last_tmp][cdn_tmp] = {};
                                        for(var j=0;j<type_all.length;j++)
                                        {
                                            var type_tmp = type_all[j];
                                            final_time_cdn_output[time_last_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_last_tmp][cdn_tmp][type_tmp];
                                        }
                                    }
                                }
                                //加入极值
                                for(var k=0;k<cdn_all.length;k++)
                                {
                                    var time_tmp = max_bw_time[cdn_all[k]];
                                    if(!final_time_cdn_output.hasOwnProperty(time_tmp))
                                    {
                                        final_time_cdn_output[time_tmp] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_tmp][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                final_time_cdn_output[time_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_tmp][cdn_tmp][type_tmp];
                                            }
                                        }
                                    }
                                    var time_tmp = min_bw_time[cdn_all[k]];
                                    if(!final_time_cdn_output.hasOwnProperty(time_tmp))
                                    {
                                        final_time_cdn_output[time_tmp] = {};
                                        for(var i=0;i<cdn_all.length;i++)
                                        {
                                            var cdn_tmp = cdn_all[i];
                                            final_time_cdn_output[time_tmp][cdn_tmp] = {};
                                            for(var j=0;j<type_all.length;j++)
                                            {
                                                var type_tmp = type_all[j];
                                                final_time_cdn_output[time_tmp][cdn_tmp][type_tmp] = time_cdn_output[time_tmp][cdn_tmp][type_tmp];
                                            }
                                        }
                                    }
                                }
                                
                                res.json(final_time_cdn_output);
                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
// } liupan add, 2017/7/20

var get_channel_cdn_func = function(req,res){
    var output={};
    try{
        connect_mongo(res,function(db){
                db.collection('user_channel_cdn_single',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=req.body.cdn;
                        var channel_tmp=req.body.channel;
                        console.log(channel_tmp);
                        var back = {
                            "_id":0,
                            "time":1,
                        }
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                            back['cdn']=1;
                        }
                        if(channel_tmp != 0)
                        {
                            query['channel']=channel_tmp;
                            back['channel']=1;
                        }
                        //console.log(query);
                        type=req.body.history_type;

                        // { liupan add, 2018/3/9
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                            query['cdn'] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/9
                        
                        back[type]=1;
                        // { liupan add, 2017/8/18
                        if (type == "band_width") {
                            back["sy_band_width"] = 1;
                        }
                        // } liupan add, 2017/8/18
                        // { liupan add, 2018/6/19
                        else if (type == "ps_freeze_rate") {
                            back["freeze_rate"] = 1;
                        }
                        // } liupan add, 2018/6/19
                        //console.log(back);
                        
                        // { liupan modify, 2017/8/18
                        // var max_min_value_dic={'max':-999999,'min':9999999999};
                        var max_min_value_dic = {};
                        if (type == "band_width") {
                            max_min_value_dic={'max':{'all':{'band_width':-999999},'sy':{'band_width':0}},'min':{'all':{'band_width':9999999999},'sy':{'band_width':0}}};
                        }
                        // { liupan add, 2018/6/19
                        else if (type == "ps_freeze_rate") {
                            max_min_value_dic={'max':{'all':{'freeze_rate':-999999},'ps':{'freeze_rate':0}},'min':{'all':{'freeze_rate':9999999999},'ps':{'freeze_rate':0}}};
                        }
                        // } liupan add, 2018/6/19
                        else {
                            max_min_value_dic={'max':-999999,'min':9999999999};
                        }
                        // } liupan modify, 2017/8/18
                        // { liupan add, 2018/1/18
                        var agent_types = ["bandwidth-new", "h5-bandwidth", "h5-bandwidth-new", "android-hls-bandwidth", "android-hls-bandwidth-new", "android-pzsp-bandwidth", "android-pzsp-bandwidth-new", "ios-hls-bandwidth", "ios-hls-bandwidth-new", "pc-flv-bandwidth", "pc-flv-bandwidth-new", "pc-hls-bandwidth", "pc-hls-bandwidth-new", "pc-hds-bandwidth", "pc-hds-bandwidth-new"];
                        // } liupan add, 2018/1/18
                        var max_min_time_dic={'max':0,'min':0};
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};

                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    if (output[data_temp["time"]])
                                    {
                                        {
                                            // { liupan modify, 2017/8/18
                                            // output[data_temp["time"]]+=data_temp[type];
                                            if (type == "band_width") {
                                                output[data_temp["time"]]["all"]["band_width"] += data_temp["band_width"];
                                                if ("sy_band_width" in data_temp)
                                                    output[data_temp["time"]]["sy"]["band_width"] += data_temp["sy_band_width"];

                                                // { liupan add, 2018/1/18
                                                for (var at = 0; at < agent_types.length; ++at) {
                                                    var agent = agent_types[at];

                                                    if ("agent"in data_temp && agent in data_temp["agent"])
                                                        output[data_temp["time"]]["agent"][agent] += data_temp["agent"][agent];
                                                }
                                                // } liupan add, 2018/1/18
                                            }
                                            // { liupan add, 2018/6/19
                                            else if (type == "ps_freeze_rate") {
                                                output[data_temp["time"]]["all"]["freeze_rate"] += data_temp["freeze_rate"];
                                                if ("ps_freeze_rate" in data_temp)
                                                    output[data_temp["time"]]["ps"]["freeze_rate"] += data_temp["ps_freeze_rate"];
                                            }
                                            // } liupan add, 2018/6/19
                                            else {
                                                output[data_temp["time"]]+=data_temp[type];
                                            }
                                            // } liupan modify, 2017/8/18
                                            out_count[data_temp["time"]]+=1;
                                        }
                                    }
                                    else
                                    {
                                        // { liupan modify, 2017/8/18
                                        // output[data_temp["time"]]=data_temp[type];
                                        if (type == "band_width") {
                                            // { liupan modify, 2018/1/18
                                            // output[data_temp["time"]] = {"all":{"band_width":0}, "sy":{"band_width":0}};
                                            output[data_temp["time"]] = {"all":{"band_width":0}, "sy":{"band_width":0}, "agent":{}};

                                            for (var at = 0; at < agent_types.length; ++at) {
                                                var agent = agent_types[at];

                                                if ("agent"in data_temp && agent in data_temp["agent"])
                                                    output[data_temp["time"]]["agent"][agent] = data_temp["agent"][agent];
                                                else
                                                    output[data_temp["time"]]["agent"][agent] = 0;
                                            }
                                            // } liupan modify, 2018/1/18
                                            output[data_temp["time"]]["all"]["band_width"] = data_temp["band_width"];
                                            if ("sy_band_width" in data_temp)
                                                output[data_temp["time"]]["sy"]["band_width"] = data_temp["sy_band_width"];
                                            else
                                                output[data_temp["time"]]["sy"]["band_width"] = 0;
                                        }
                                        // { liupan add, 2018/6/19
                                        else if (type == "ps_freeze_rate") {
                                            output[data_temp["time"]] = {"all":{"freeze_rate":0}, "ps":{"freeze_rate":0}};

                                            output[data_temp["time"]]["all"]["freeze_rate"] = data_temp["freeze_rate"];
                                            if ("ps_freeze_rate" in data_temp)
                                                output[data_temp["time"]]["ps"]["freeze_rate"] = data_temp["ps_freeze_rate"];
                                            else
                                                output[data_temp["time"]]["ps"]["freeze_rate"] = 0;
                                        }
                                        // } liupan add, 2018/6/19
                                        else {
                                            output[data_temp["time"]]=data_temp[type];
                                        }
                                        // } liupan modify, 2017/8/18
                                        out_count[data_temp["time"]]=1;
                                    }
                                }
                                var band_arr=[];
                                // { liupan add, 2017/8/18
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                var sy_value_sum=0;
                                // } liupan add, 2017/8/18
                                // { liupan add, 2018/1/18
                                var agent_value_sum = {};
                                for (var at = 0; at < agent_types.length; ++at) {
                                    agent_value_sum[agent_types[at]] = 0;
                                }
                                // } liupan add, 2018/1/18
                                if(type=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        // { liupan modify, 2017/8/18
                                        // band_arr.push(output[t]);
                                        // var data_time_stamp=t;
                                        // var data_value=output[t];
                                        // if(data_value>max_min_value_dic['max'])
                                        // {
                                        //     max_min_value_dic['max']=data_value;
                                        //     max_min_time_dic['max']=data_time_stamp;
                                        // }
                                        // else if(data_value < max_min_value_dic['min'])
                                        // {
                                        //     max_min_value_dic['min']=data_value;
                                        //     max_min_time_dic['min']=data_time_stamp;
                                        // }
                                        band_arr.push(output[t]["all"]["band_width"]);
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["band_width"];
                                        if (data_value > max_min_value_dic['max']["all"]["band_width"]) {
                                            // { liupan modify, 2018/1/18
                                            // max_min_value_dic['max']["all"]["band_width"] = data_value;
                                            max_min_value_dic['max'] = output[t];
                                            // } liupan modify, 2018/1/18
                                            //max_min_value_dic['max']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                            // { liupan modify, 2018/1/18
                                            // max_min_value_dic['min']["all"]["band_width"] = data_value;
                                            max_min_value_dic['min'] = output[t];
                                            // } liupan modify, 2018/1/18
                                            //max_min_value_dic['min']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["band_width"];
                                        sy_value_sum += output[t]["sy"]["band_width"];
                                        // { liupan add, 2018/1/18
                                        for (var at in output[t]["agent"]) {
                                            agent_value_sum[at] += output[t]["agent"][at];
                                        }
                                        // } liupan add, 2018/1/18
                                        if (value_count >= LIMIT_COUNT) {
                                            // { liupan modify, 2018/1/18
                                            // out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};
                                            out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}, 'agent':{}};

                                            for (var at in agent_value_sum) {
                                                out_result[value_first_time]['agent'][at] = agent_value_sum[at] / value_sum * 100;
                                                agent_value_sum[at] = 0;
                                            }
                                            // } liupan modify, 2018/1/18
                                            out_result[value_first_time]['all']['band_width'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['sy']['band_width'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                        // } liupan modify, 2017/8/18
                                    }
                                }
                                // { liupan add, 2018/6/19
                                else if (type == "ps_freeze_rate") {
                                    for(var t in output)
                                    {
                                        if (out_count[t] != 0) {
                                            output[t] = output[t] / out_count[t];
                                        }
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["freeze_rate"];
                                        if (data_value > max_min_value_dic['max']["all"]["freeze_rate"]) {
                                            max_min_value_dic['max'] = output[t];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["freeze_rate"]) {
                                            max_min_value_dic['min'] = output[t];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["freeze_rate"];
                                        sy_value_sum += output[t]["ps"]["freeze_rate"];

                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};

                                            out_result[value_first_time]['all']['freeze_rate'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / LIMIT_COUNT;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                    }
                                }
                                // } liupan add, 2018/6/19
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                        {
                                            output[t]=output[t]/out_count[t];
                                        }
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        // { liupan add, 2017/8/18
                                        ori_count+=1;
                                        if(value_count==0)
                                        {
                                            value_first_time=t;
                                        }
                                        value_count+=1;
                                        value_sum+=output[t];
                                        if(value_count >=LIMIT_COUNT)
                                        {
                                            out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                            test_count+=1;
                                            value_sum=0;
                                            value_count=0;
                                        }
                                        // } liupan add, 2017/8/18
                                    }
                                }
                                // { liupan delete, 2017/8/18
                                // var value_count=0;
                                // var value_sum=0;
                                // var value_first_time=0;
                                // var out_result={};
                                // var test_count=0;
                                // var ori_count=0;
                                // for(var tt in output)
                                // {
                                //     ori_count+=1;
                                //     if(value_count==0)
                                //     {
                                //         value_first_time=tt;
                                //     }
                                //     value_count+=1;
                                //     value_sum+=output[tt];
                                //     if(value_count >=LIMIT_COUNT)
                                //     {
                                //         out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                //         test_count+=1;
                                //         value_sum=0;
                                //         value_count=0;
                                //     }
                                // }
                                // } liupan delete, 2017/8/18
                                // { liupan modify, 2018/6/19
                                // out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                // out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                for (var xxx in out_result) {
                                    out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                    out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                    break;
                                }
                                // } liupan modify, 2018/6/19
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        all_output["detail"]=out_result;
                                        all_output["bandwidth95"]=band95;
                                        break;
                                    // { liupan modify, 2018/6/19
                                    // case "bitrate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "freeze_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "success_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "ps_freeze_rate":
                                    case "freeze_avg_iv":
                                    case "delayed_avg":
                                        all_output["detail"] = out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/19
                                }
                                res.json(all_output);

                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

var get_user_history_func = function(req,res){
    var output={};

    try{
        connect_mongo(res,function(db){
                db.collection('history_user_single',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=req.body.cdn;
                        var channel_tmp=req.body.channel;
                        console.log(channel_tmp);
                        var back = {
                            "_id":0,
                            "time":1,
                        }
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                            back['region']=1;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                            back['operator']=1;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                            back['cdn']=1;
                        }

                        if(channel_tmp != 0)
                        {
                            query['channel']=channel_tmp;
                            back['channel']=1;
                        }
                        //console.log(query);
                        type=req.body.history_type;

                        // { liupan add, 2018/3/9
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                            query['cdn'] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/9
                        if (type == "jamnumperminute"){
                            back['jam_all'] = 1;
                            back['duration'] = 1;
                        }else {
                            back[type]=1;
                        }
                        // { liupan add, 2017/8/18
                        if (type == "band_width") {
                            back["sy_band_width"] = 1;
                        }
                        // } liupan add, 2017/8/18
                        // { liupan add, 2018/6/19
                        else if (type == "ps_freeze_rate") {
                            back["freeze_rate"] = 1;
                        }
                        // } liupan add, 2018/6/19
                        //console.log(back);
                        
                        // { liupan modify, 2017/8/18
                        // var max_min_value_dic={'max':-999999,'min':9999999999};
                        var max_min_value_dic = {};
                        if (type == "band_width") {
                            max_min_value_dic={'max':{'all':{'band_width':-999999},'sy':{'band_width':0}},'min':{'all':{'band_width':9999999999},'sy':{'band_width':0}}};
                        }
                        // { liupan add, 2018/6/19
                        else if (type == "ps_freeze_rate") {
                            max_min_value_dic = {'max':{'all':{'freeze_rate':-999999},'ps':{'freeze_rate':0}},'min':{'all':{'freeze_rate':9999999999},'ps':{'freeze_rate':0}}};
                        }
                        // } liupan add, 2018/6/19
                        else {
                            max_min_value_dic={'max':-999999,'min':9999999999};
                        }
                        // } liupan modify, 2017/8/18
                        var max_min_time_dic={'max':0,'min':0};
                        logger.info(query);
                        logger.info(back);
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};
                                var sum_jam = 0;
                                var sum_duration = 0;

                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    // { liupan add, 2018/6/12

                                    if (type=="jamnumperminute"){
                                        var data_temp=logs[i];
                                        var data_time_stamp=data_temp["time"];
                                        var jam_all = data_temp['jam_all']  ? parseInt(data_temp['jam_all']) : 0;
                                        var duration = data_temp['duration'] ? parseInt(data_temp['duration']) : 0;
                                        sum_jam += jam_all;
                                        sum_duration += duration;


                                        if (duration==0){
                                            data_value = 0
                                        }else {
                                            data_value = jam_all / (duration /1000 /60);
                                        };
                                        if (output[data_time_stamp])
                                        {
                                            output[data_time_stamp]+=data_value;
                                            out_count[data_time_stamp]+=1;
                                        }
                                        else {
                                            output[data_time_stamp]=data_value;
                                            out_count[data_time_stamp]=1;
                                        }
                                    }
                                    else {
                                        if (!(type in data_temp)) continue;
                                        // } liupan add, 2018/6/12
                                        if (output[data_temp["time"]]) {
                                            {
                                                // { liupan modify, 2017/8/18
                                                // output[data_temp["time"]]+=data_temp[type];
                                                if (type == "band_width") {
                                                    output[data_temp["time"]]["all"]["band_width"] += data_temp["band_width"];
                                                    if ("sy_band_width" in data_temp)
                                                        output[data_temp["time"]]["sy"]["band_width"] += data_temp["sy_band_width"];
                                                }
                                                // { liupan add, 2018/6/19
                                                else if (type == "ps_freeze_rate") {
                                                    output[data_temp["time"]]["all"]["freeze_rate"] += data_temp["freeze_rate"];
                                                    if ("ps_freeze_rate" in data_temp)
                                                        output[data_temp["time"]]["ps"]["freeze_rate"] += data_temp["ps_freeze_rate"];
                                                }
                                                // } liupan add, 2018/6/19
                                                else {
                                                    output[data_temp["time"]] += data_temp[type];
                                                }
                                                // } liupan modify, 2017/8/18
                                                out_count[data_temp["time"]] += 1;
                                            }
                                        }
                                        else {
                                            // { liupan modify, 2017/8/18
                                            // output[data_temp["time"]]=data_temp[type];
                                            if (type == "band_width") {
                                                output[data_temp["time"]] = {
                                                    "all": {"band_width": 0},
                                                    "sy": {"band_width": 0}
                                                };
                                                output[data_temp["time"]]["all"]["band_width"] = data_temp["band_width"];
                                                if ("sy_band_width" in data_temp)
                                                    output[data_temp["time"]]["sy"]["band_width"] = data_temp["sy_band_width"];
                                                else
                                                    output[data_temp["time"]]["sy"]["band_width"] = 0;
                                            }
                                            // { liupan add, 2018/6/19
                                            else if (type == "ps_freeze_rate") {
                                                output[data_temp["time"]] = {
                                                    "all": {"freeze_rate": 0},
                                                    "ps": {"freeze_rate": 0}
                                                };
                                                output[data_temp["time"]]["all"]["freeze_rate"] = data_temp["freeze_rate"];
                                                if ("ps_freeze_rate" in data_temp)
                                                    output[data_temp["time"]]["ps"]["freeze_rate"] = data_temp["ps_freeze_rate"];
                                                else
                                                    output[data_temp["time"]]["ps"]["freeze_rate"] = 0;
                                            }
                                            // } liupan add, 2018/6/19
                                            else {
                                                output[data_temp["time"]] = data_temp[type];
                                            }
                                            // } liupan modify, 2017/8/18
                                            out_count[data_temp["time"]] = 1;
                                        }
                                    }
                                }
                                var band_arr=[];
                                // { liupan add, 2017/12/12
                                var band_4thDayPeak = {};   // 用于第四日峰值带宽统计方法：记录每一天的最大值
                                var band_4thDayPeak_arr = [];
                                // } liupan add, 2017/12/12
                                // { liupan add, 2017/8/18
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                var sy_value_sum=0;
                                // } liupan add, 2017/8/18
                                if(type=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        // { liupan modify, 2017/8/18
                                        // band_arr.push(output[t]);
                                        // var data_time_stamp=t;
                                        // var data_value=output[t];
                                        // if(data_value>max_min_value_dic['max'])
                                        // {
                                        //     max_min_value_dic['max']=data_value;
                                        //     max_min_time_dic['max']=data_time_stamp;
                                        // }
                                        // else if(data_value < max_min_value_dic['min'])
                                        // {
                                        //     max_min_value_dic['min']=data_value;
                                        //     max_min_time_dic['min']=data_time_stamp;
                                        // }
                                        band_arr.push(output[t]["all"]["band_width"]);
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["band_width"];
                                        if (data_value > max_min_value_dic['max']["all"]["band_width"]) {
                                            max_min_value_dic['max']["all"]["band_width"] = data_value;
                                            //max_min_value_dic['max']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        // { liupan modify, 2018/6/13
                                        // else if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                        if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                        // } liupan modify, 2018/6/13
                                            max_min_value_dic['min']["all"]["band_width"] = data_value;
                                            //max_min_value_dic['min']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["band_width"];
                                        sy_value_sum += output[t]["sy"]["band_width"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};
                                            out_result[value_first_time]['all']['band_width'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['sy']['band_width'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                        // } liupan modify, 2017/8/18
                                        // { liupan add, 2017/12/12
                                        var date = new Date(t * 1000);
                                        var ymd = "" + date.getFullYear() + date.getMonth() + date.getDate();
                                        if (!(ymd in band_4thDayPeak)) {
                                            band_4thDayPeak[ymd] = output[t]["all"]["band_width"];
                                        }
                                        else {
                                            if (band_4thDayPeak[ymd] < output[t]["all"]["band_width"])
                                                band_4thDayPeak[ymd] = output[t]["all"]["band_width"];
                                        }
                                        // } liupan add, 2017/12/12
                                    }
                                    // { liupan add, 2018/6/13
                                    if (value_count != 0) {
                                        out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};
                                        out_result[value_first_time]['all']['band_width'] = value_sum / value_count;
                                        out_result[value_first_time]['sy']['band_width'] = sy_value_sum / value_count;
                                    }
                                    // } liupan add, 2018/6/13

                                    // { liupan add, 2017/12/12
                                    for (var ymd in band_4thDayPeak) {
                                        band_4thDayPeak_arr.push(band_4thDayPeak[ymd]);
                                    }
                                    // } liupan add, 2017/12/12
                                }
                                // { liupan add, 2018/6/19
                                else if (type == 'ps_freeze_rate')
                                {
                                    for(var t in output)
                                    {
                                        if (out_count[t] != 0) {
                                            output[t] = output[t] / out_count[t];
                                        }
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["freeze_rate"];
                                        if (data_value > max_min_value_dic['max']["all"]["freeze_rate"]) {
                                            max_min_value_dic['max']["all"]["freeze_rate"] = data_value;
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["freeze_rate"]) {
                                            max_min_value_dic['min']["all"]["freeze_rate"] = data_value;
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["freeze_rate"];
                                        sy_value_sum += output[t]["ps"]["freeze_rate"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};
                                            out_result[value_first_time]['all']['freeze_rate'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                    }
                                    if (value_count != 0) {
                                        out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};
                                        out_result[value_first_time]['all']['freeze_rate'] = value_sum / value_count;
                                        out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / value_count;
                                    }
                                }
                                // } liupan add, 2018/6/19
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                        {
                                            // { liupan add, 2018/6/12
                                            if (type != "user_n" && type != "req_n")
                                            // } liupan add, 2018/6/12
                                            output[t]=output[t]/out_count[t];
                                        }
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        // { liupan modify, 2018/6/13
                                        // else if(data_value < max_min_value_dic['min'])
                                        if (data_value < max_min_value_dic['min'])
                                        // } liupan modify, 2018/6/13
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        // { liupan add, 2017/8/18
                                        ori_count+=1;
                                        if(value_count==0)
                                        {
                                            value_first_time=t;
                                        }
                                        value_count+=1;
                                        value_sum+=output[t];
                                        if(value_count >=LIMIT_COUNT)
                                        {
                                            out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                            test_count+=1;
                                            value_sum=0;
                                            value_count=0;
                                        }
                                        // } liupan add, 2017/8/18
                                    }
                                    // { liupan add, 2018/6/13
                                    if (value_count != 0) {
                                        out_result[value_first_time] = value_sum / value_count;
                                    }
                                    // } liupan add, 2018/6/13
                                }
                                // { liupan delete, 2017/8/18
                                // var value_count=0;
                                // var value_sum=0;
                                // var value_first_time=0;
                                // var out_result={};
                                // var test_count=0;
                                // var ori_count=0;
                                // for(var tt in output)
                                // {
                                //     ori_count+=1;
                                //     if(value_count==0)
                                //     {
                                //         value_first_time=tt;
                                //     }
                                //     value_count+=1;
                                //     value_sum+=output[tt];
                                //     if(value_count >=LIMIT_COUNT)
                                //     {
                                //         out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                //         test_count+=1;
                                //         value_sum=0;
                                //         value_count=0;
                                //     }
                                // }
                                // } liupan delete, 2017/8/18
                                // { liupan modify, 2018/6/13
                                // out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                // out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                for (var xxx in out_result) {
                                    out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                    out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                    break;
                                }
                                // } liupan modify, 2018/6/13
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        // { liupan add, 2017/12/12
                                        var sort_b4dp = quickSort(band_4thDayPeak_arr);
                                        var band4dp = 0;
                                        if (sort_b4dp.length >= 4) {
                                            band4dp = sort_b4dp[sort_b4dp.length - 4];
                                        }
                                        else {
                                            band4dp = sort_b4dp[0];
                                        }
                                        // } liupan add, 2017/12/12
                                        all_output["detail"]=out_result;
                                        // { liupan modify, 2017/12/12
                                        // all_output["band95"]=band95;
                                        if (config.bandwidth_measurement_method == 0) {
                                            all_output["bandwidth95"]=band95;
                                        }
                                        else if (config.bandwidth_measurement_method == 1) {
                                            all_output["bandwidth4dp"] = band4dp;
                                        }
                                        // } liupan modify, 2017/12/12
                                        break;
                                    case "jamnumperminute":
                                        all_output["detail"]=out_result;
                                        if (sum_duration==0){
                                            all_output["jamnumAverage"] = 0;
                                        }else{
                                            all_output["jamnumAverage"] = sum_jam / (sum_duration / 1000 / 60);
                                        }
                                    // { liupan modify, 2018/6/12
                                    // case "bitrate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "freeze_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "success_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "user_n":
                                    case "req_n":
                                    // { liupan add, 2018/6/19
                                    case "ps_freeze_rate":
                                    // } liupan add, 2018/6/19
                                    case "freeze_avg_iv":
                                    case "delayed_avg":
                                        all_output["detail"]=out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/12
                                }
                                res.json(all_output);

                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}


var get_user_sum_history_func = function(req,res){
    var output={};
    try{
        connect_mongo(res,function(db){
                db.collection('history_user_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        type=req.body.history_type;
                        // { liupan add, 2018/3/9
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                        }
                        // } liupan add, 2018/3/9

                        var back = {
                            "_id":0,
                            "time":1,
                            // "region":1,
                            // "operator":1,
                            "cdn":1
                        }
                        if (type == "jamnumperminute"){
                            back['jam_all'] = 1;
                            back['duration'] = 1;
                        }else {
                            back[type]=1;
                        }
                        // { liupan add, 2017/8/18
                        if (type == "band_width") {
                            back["sy_band_width"] = 1;
                        }
                        // } liupan add, 2017/8/18
                        // { liupan add, 2018/6/19
                        if (type == "ps_freeze_rate") {
                            back["freeze_rate"] = 1;
                        }
                        // } liupan add, 2018/6/19
                        //console.log(back);
                        
                        // { liupan modify, 2017/8/18
                        // var max_min_value_dic={'max':-999999,'min':9999999999};
                        var max_min_value_dic = {};
                        if (type == "band_width") {
                            max_min_value_dic={'max':{'all':{'band_width':-999999},'sy':{'band_width':0}},'min':{'all':{'band_width':9999999999},'sy':{'band_width':0}}};
                        }
                        // { liupan add, 2018/6/19
                        else if (type == "ps_freeze_rate") {
                            max_min_value_dic={'max':{'all':{'freeze_rate':-999999},'ps':{'freeze_rate':0}},'min':{'all':{'freeze_rate':9999999999},'ps':{'freeze_rate':0}}};
                        }
                        // } liupan add, 2018/6/19
                        else {
                            max_min_value_dic={'max':-999999,'min':9999999999};
                        }
                        // } liupan modify, 2017/8/18
                        var max_min_time_dic={'max':0,'min':0};
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};
                                var sum_jam = 0;
                                var sum_duration = 0;

                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    // { liupan add, 2018/6/12

                                    if (type=="jamnumperminute"&&cdn_temp==0){
                                        var data_time_stamp=data_temp["time"];
                                        var jam_all = data_temp['jam_all']  ? parseInt(data_temp['jam_all']) : 0;
                                        var duration = data_temp['duration'] ? parseInt(data_temp['duration']) : 0;
                                        sum_jam += jam_all;
                                        sum_duration += duration;
                                        if (duration==0){
                                            data_value = 0
                                        }else {
                                            data_value = jam_all / (duration /1000 /60);
                                        };
                                        if (output[data_time_stamp]) //累加
                                        {
                                            output[data_time_stamp]+=data_value;
                                            out_count[data_time_stamp]+=1;
                                        }
                                        else
                                        {
                                            output[data_time_stamp]=data_value;
                                            out_count[data_time_stamp]=1;

                                        }

                                    }else {
                                        if (!(type in data_temp)) continue;
                                        // } liupan add, 2018/6/12
                                        // { liupan add, 2018/3/9
                                        if (cdn_list.length > 0) {
                                            data_temp = {"time": logs[i]['time'], "band_width": 0, "sy_band_width": 0};
                                            // { liupan add, 2018/6/19
                                            data_temp["freeze_rate"] = 0;
                                            // } liupan add, 2018/6/19
                                            data_temp[type] = 0;
                                            var valid_count = 0;
                                            for (var j = 0; j < cdn_list.length; ++j) {
                                                if (!(cdn_list[j] in logs[i]['cdn']) || !(type in logs[i]['cdn'][cdn_list[j]])) continue;

                                                if (type == "band_width") {
                                                    data_temp["band_width"] += logs[i]['cdn'][cdn_list[j]]["band_width"];
                                                    if ("sy_band_width" in logs[i]['cdn'][cdn_list[j]])
                                                        data_temp["sy_band_width"] += logs[i]['cdn'][cdn_list[j]]["sy_band_width"];
                                                }
                                                // { liupan add, 2018/6/19
                                                else if (type == "ps_freeze_rate") {
                                                    data_temp["freeze_rate"] += logs[i]['cdn'][cdn_list[j]]["freeze_rate"];
                                                    if ("ps_freeze_rate" in logs[i]['cdn'][cdn_list[j]])
                                                        data_temp["ps_freeze_rate"] += logs[i]['cdn'][cdn_list[j]]["ps_freeze_rate"];
                                                }
                                                // } liupan add, 2018/6/19
                                                else {
                                                    data_temp[type] += logs[i]['cdn'][cdn_list[j]][type];
                                                }

                                                if (logs[i]['cdn'][cdn_list[j]]["band_width"] > 0)
                                                    valid_count++;
                                            }

                                            if (type == "success_rate" || type == "freeze_rate" || type == "bitrate") {
                                                if (valid_count > 0)
                                                    data_temp[type] /= valid_count;
                                            }
                                            // { liupan add, 2018/6/19
                                            else if (type == "ps_freeze_rate") {
                                                if (valid_count > 0) {
                                                    data_temp["freeze_rate"] /= valid_count;
                                                    data_temp["ps_freeze_rate"] /= valid_count;
                                                }
                                            }
                                            // } liupan add, 2018/6/19
                                        }
                                        // } liupan add, 2018/3/9
                                        if (output[data_temp["time"]]) {
                                            {
                                                // { liupan modify, 2017/8/18
                                                // output[data_temp["time"]]+=data_temp[type];
                                                if (type == "band_width") {
                                                    output[data_temp["time"]]["all"]["band_width"] += data_temp["band_width"];
                                                    if ("sy_band_width" in data_temp)
                                                        output[data_temp["time"]]["sy"]["band_width"] += data_temp["sy_band_width"];
                                                }
                                                // { liupan add, 2018/6/19
                                                else if (type == "ps_freeze_rate") {
                                                    output[data_temp["time"]]["all"]["freeze_rate"] += data_temp["freeze_rate"];
                                                    if ("ps_freeze_rate" in data_temp)
                                                        output[data_temp["time"]]["ps"]["freeze_rate"] += data_temp["ps_freeze_rate"];
                                                }
                                                // } liupan add, 2018/6/19
                                                else {
                                                    output[data_temp["time"]] += data_temp[type];
                                                }
                                                // } liupan modify, 2017/8/18
                                                out_count[data_temp["time"]] += 1;
                                            }
                                        }
                                        else {
                                            // { liupan modify, 2017/8/18
                                            // output[data_temp["time"]]=data_temp[type];
                                            if (type == "band_width") {
                                                output[data_temp["time"]] = {
                                                    "all": {"band_width": 0},
                                                    "sy": {"band_width": 0}
                                                };
                                                output[data_temp["time"]]["all"]["band_width"] = data_temp["band_width"];
                                                if ("sy_band_width" in data_temp)
                                                    output[data_temp["time"]]["sy"]["band_width"] = data_temp["sy_band_width"];
                                                else
                                                    output[data_temp["time"]]["sy"]["band_width"] = 0;
                                            }
                                            // { liupan add, 2018/6/19
                                            else if (type == "ps_freeze_rate") {
                                                output[data_temp["time"]] = {
                                                    "all": {"freeze_rate": 0},
                                                    "ps": {"freeze_rate": 0}
                                                };
                                                output[data_temp["time"]]["all"]["freeze_rate"] = data_temp["freeze_rate"];
                                                if ("ps_freeze_rate" in data_temp)
                                                    output[data_temp["time"]]["ps"]["freeze_rate"] = data_temp["ps_freeze_rate"];
                                                else
                                                    output[data_temp["time"]]["ps"]["freeze_rate"] = 0;
                                            }
                                            // } liupan add, 2018/6/19
                                            else {
                                                output[data_temp["time"]] = data_temp[type];
                                            }
                                            // } liupan modify, 2017/8/18
                                            out_count[data_temp["time"]] = 1;
                                        }
                                    }
                                }
                                var band_arr=[];
                                // { liupan add, 2017/12/12
                                var band_4thDayPeak = {};   // 用于第四日峰值带宽统计方法：记录每一天的最大值
                                var band_4thDayPeak_arr = [];
                                // } liupan add, 2017/12/12
                                // { liupan add, 2017/8/18
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                var sy_value_sum=0;
                                // } liupan add, 2017/8/18
                                if(type=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        // { liupan modify, 2017/8/18
                                        // band_arr.push(output[t]);
                                        // var data_time_stamp=t;
                                        // var data_value=output[t];
                                        // if(data_value>max_min_value_dic['max'])
                                        // {
                                        //     max_min_value_dic['max']=data_value;
                                        //     max_min_time_dic['max']=data_time_stamp;
                                        // }
                                        // else if(data_value < max_min_value_dic['min'])
                                        // {
                                        //     max_min_value_dic['min']=data_value;
                                        //     max_min_time_dic['min']=data_time_stamp;
                                        // }
                                        band_arr.push(output[t]["all"]["band_width"]);
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["band_width"];
                                        if (data_value > max_min_value_dic['max']["all"]["band_width"]) {
                                            max_min_value_dic['max']["all"]["band_width"] = data_value;
                                            //max_min_value_dic['max']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                            max_min_value_dic['min']["all"]["band_width"] = data_value;
                                            //max_min_value_dic['min']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["band_width"];
                                        sy_value_sum += output[t]["sy"]["band_width"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};
                                            out_result[value_first_time]['all']['band_width'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['sy']['band_width'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                        // } liupan modify, 2017/8/18
                                        // { liupan add, 2017/12/12
                                        var date = new Date(t * 1000);
                                        var ymd = "" + date.getFullYear() + date.getMonth() + date.getDate();
                                        if (!(ymd in band_4thDayPeak)) {
                                            band_4thDayPeak[ymd] = output[t]["all"]["band_width"];
                                        }
                                        else {
                                            if (band_4thDayPeak[ymd] < output[t]["all"]["band_width"])
                                                band_4thDayPeak[ymd] = output[t]["all"]["band_width"];
                                        }
                                        // } liupan add, 2017/12/12
                                    }
                                    // { liupan add, 2017/12/12
                                    for (var ymd in band_4thDayPeak) {
                                        band_4thDayPeak_arr.push(band_4thDayPeak[ymd]);
                                    }
                                    // } liupan add, 2017/12/12
                                }
                                // { liupan add, 2018/6/19
                                else if (type == "ps_freeze_rate") {
                                    for(var t in output)
                                    {
                                        if (out_count[t] != 0) {
                                            output[t] = output[t] / out_count[t];
                                        }
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["freeze_rate"];
                                        if (data_value > max_min_value_dic['max']["all"]["freeze_rate"]) {
                                            max_min_value_dic['max']["all"]["freeze_rate"] = data_value;
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["freeze_rate"]) {
                                            max_min_value_dic['min']["all"]["freeze_rate"] = data_value;
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if (value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["freeze_rate"];
                                        sy_value_sum += output[t]["ps"]["freeze_rate"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};
                                            out_result[value_first_time]['all']['freeze_rate'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / LIMIT_COUNT;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                    }
                                }
                                // } liupan add, 2018/6/19
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                        {
                                            // { liupan add, 2018/6/12
                                            if (type != 'user_n' && type != 'req_n') 
                                            // } liupan add, 2018/6/12
                                            output[t]=output[t]/out_count[t];
                                        }
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min'])
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        // { liupan add, 2017/8/18
                                        ori_count+=1;
                                        if(value_count==0)
                                        {
                                            value_first_time=t;
                                        }
                                        value_count+=1;
                                        value_sum+=output[t];
                                        if(value_count >=LIMIT_COUNT)
                                        {
                                            out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                            test_count+=1;
                                            value_sum=0;
                                            value_count=0;
                                        }
                                        // } liupan add, 2017/8/18
                                    }
                                }
                                // { liupan delete, 2017/8/18
                                // var value_count=0;
                                // var value_sum=0;
                                // var value_first_time=0;
                                // var out_result={};
                                // var test_count=0;
                                // var ori_count=0;
                                // for(var tt in output)
                                // {
                                //     ori_count+=1;
                                //     if(value_count==0)
                                //     {
                                //         value_first_time=tt;
                                //     }
                                //     value_count+=1;
                                //     value_sum+=output[tt];
                                //     if(value_count >=LIMIT_COUNT)
                                //     {
                                //         out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                //         test_count+=1;
                                //         value_sum=0;
                                //         value_count=0;
                                //     }
                                // }
                                // } liupan delete, 2017/8/18
                                // { liupan modify, 2018/6/19
                                // out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                // out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                for (var xxx in out_result) {
                                    out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                    out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                    break;
                                }
                                // } liupan modify, 2018/6/19
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        // { liupan add, 2017/12/12
                                        var sort_b4dp = quickSort(band_4thDayPeak_arr);
                                        var band4dp = 0;
                                        if (sort_b4dp.length >= 4) {
                                            band4dp = sort_b4dp[sort_b4dp.length - 4];
                                        }
                                        else {
                                            band4dp = sort_b4dp[0];
                                        }
                                        // } liupan add, 2017/12/12
                                        all_output["detail"]=out_result;
                                        // { liupan modify, 2017/12/12
                                        // all_output["band95"]=band95;
                                        if (config.bandwidth_measurement_method == 0) {
                                            all_output["bandwidth95"]=band95;
                                        }
                                        else if (config.bandwidth_measurement_method == 1) {
                                            all_output["bandwidth4dp"] = band4dp;
                                        }
                                        // } liupan modify, 2017/12/12
                                        break;
                                    case "jamnumperminute":
                                        all_output["detail"]=out_result;
                                        if (sum_duration==0){
                                            all_output["jamnumAverage"] = 0;
                                        }else{
                                            all_output["jamnumAverage"] = sum_jam / (sum_duration / 1000 / 60);
                                        }
                                    // { liupan modify, 2018/6/12
                                    // case "bitrate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "freeze_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    // case "success_rate":
                                    //     all_output["detail"]=out_result;
                                    //     break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "user_n":
                                    case "req_n":
                                    // { liupan add, 2018/6/19
                                    case "ps_freeze_rate":
                                    case "freeze_avg_iv":
                                    case "delayed_avg":                                    // } liupan add, 2018/6/19
                                        all_output["detail"]=out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/12
                                }
                                res.json(all_output);

                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

// { liupan add, 2017/8/1
var get_user_sum_cdn_history_common_func = function(req,res){
    var output={};
    var kw_output={};
    var ws_output={};
    var dl_output={};
    var limit_count=LIMIT_COUNT*3
    try{
        connect_mongo(res,function(db){
                db.collection('history_user_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=0;//req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        //type=req.body.history_type;

                        var back = {
                            "_id":0,
                            "time":1,
                            "band_width":1,
                            "edge_band_width":1,
                            "freeze_rate":1,
                            "bitrate":1,
                            "success_rate":1,
                            "agent":1,
                            //"region":1,
                            //"operator":1,
                            "cdn":1
                        }
                        //back[type]=1;
                        //console.log(back);
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err len(logs)=0');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};
                                var data_out=[];
                                var struct_sum={};
                                var struct_count={'all':{}};

                                // { liupan add, 2018/3/9
                                var cdn_list = [];
                                if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                                    cdn_list = req.body.cdn_list.split(",");
                                }
                                // } liupan add, 2018/3/9
                                
                                for (var i=0; i <logs.length;i++)
                                {
                                    if(!logs[i]['agent'])
                                    {
                                        continue;
                                    }
                                    var data_temp=logs[i]['agent'];
                                    var time_temp=logs[i]['time'];
                                    if(!struct_sum[time_temp])
                                    {
                                        struct_sum[time_temp]={};
                                    }
                                    for(var cdn_temp in data_temp)
                                    {
                                        // { liupan add, 2018/3/9
                                        if (cdn_list.length > 0 && cdn_temp != "all") {
                                            for (var j = 0; j < cdn_list.length; ++j) {
                                                if (cdn_list[j] == cdn_temp)
                                                    break;
                                            }

                                            if (j >= cdn_list.length) {
                                                continue;
                                            }
                                        }
                                        // } liupan add, 2018/3/9
                                        //console.log('cdn:',cdn_temp)
                                        if(!struct_sum[time_temp][cdn_temp])
                                        {
                                            struct_sum[time_temp][cdn_temp]={};
                                            struct_sum[time_temp][cdn_temp]['concurrent']=0;
                                            struct_sum[time_temp][cdn_temp]['edge_concurrent'] = 0;
                                        }
                                        // { liupan add, 2018/3/9
                                        if (cdn_list.length > 0 && cdn_temp == "all") {
                                            continue;
                                        }
                                        // } liupan add, 2018/3/9
                                        struct_sum[time_temp][cdn_temp]['concurrent']+=data_temp[cdn_temp]['band_width']?data_temp[cdn_temp]['band_width']/data_temp[cdn_temp]['bitrate']:0;
                                        struct_sum[time_temp][cdn_temp]['edge_concurrent']+=data_temp[cdn_temp]['edge_band_width']?data_temp[cdn_temp]['edge_band_width']/data_temp[cdn_temp]['bitrate']:0;
                                        for(var detail in data_temp[cdn_temp])
                                        {
                                            if(!struct_sum[time_temp][cdn_temp][detail])
                                            {
                                                //console.log('new data'+detail);
                                                //console.log(data_temp[cdn_temp][detail]);
                                                struct_sum[time_temp][cdn_temp][detail]=0;
                                            }
                                            struct_sum[time_temp][cdn_temp][detail]+=data_temp[cdn_temp][detail];
                                        }
                                    }

                                    // { liupan add, 2018/3/9
                                    if (cdn_list.length == 0) continue;

                                    for (var cdn_key in struct_sum[time_temp]) {
                                        if (cdn_key == "all") continue;

                                        for (var type_key in struct_sum[time_temp][cdn_key]) {
                                            if(!struct_sum[time_temp]["all"][type_key])
                                                struct_sum[time_temp]["all"][type_key] = 0;
                                            struct_sum[time_temp]["all"][type_key] += struct_sum[time_temp][cdn_key][type_key];
                                        }
                                    }
                                    // } liupan add, 2018/3/9
                                }
                                var out_temp={}
                                var value_count=0;
                                var sum_interval_temp=0;
                                var detail_agent_data={};
                                var value_first_time=0;
                                // { liupan modify, 2017/10/1
                                // var max_min={'max_time':0,'min_time':0,'max_value':0,'min_value':9999999}
                                var max_min={'max_time':0,'min_time':0,'max_value':-1,'min_value':9999999};
                                // } liupan modify, 2017/10/1
                                var last_time=0;
                                var cdn_list = cdn_detail;
                                // { liupan add, 2018/1/18
                                var agent_types = ["h5-bandwidth", "h5-bandwidth-new", "android-hls-bandwidth", "android-hls-bandwidth-new", "android-pzsp-bandwidth", "android-pzsp-bandwidth-new", "ios-hls-bandwidth", "ios-hls-bandwidth-new", "pc-flv-bandwidth", "pc-flv-bandwidth-new", "pc-hls-bandwidth", "pc-hls-bandwidth-new", "pc-hds-bandwidth", "pc-hds-bandwidth-new"];
                                // } liupan add, 2018/1/18
                                for(var time_temp in struct_sum)
                                {
                                    last_time=time_temp;
                                    cdn_dic=struct_sum[time_temp]
                                    var cdn_count=0;
                                    for(var cdn_temp in cdn_dic)
                                    {
                                        if(cdn_list.indexOf(cdn_temp)>=0)
                                        {
                                            cdn_count++;
                                        }
                                    }
                                    // { liupan delete, 2017/11/1
                                    // if(cdn_count<3)
                                    // {
                                    //     continue;
                                    // }
                                    // } liupan delete, 2017/11/1
                                    if(value_count==0)
                                    {
                                        value_first_time=time_temp;
                                    }
                                    if(max_min['max_time']==0)
                                    {
                                        max_min['max_value']=cdn_dic['all']['band_width'];
                                        max_min['max_time']=time_temp;
                                        max_min['min_value']=cdn_dic['all']['band_width'];
                                        max_min['min_time']=time_temp;
                                    }
                                    if(cdn_dic['all']['band_width']>max_min['max_value'])
                                    {
                                        max_min['max_value']=cdn_dic['all']['band_width'];
                                        max_min['max_time']=time_temp;
                                    }
                                    if (cdn_dic['all']['band_width']<max_min['min_value'])
                                    {
                                        max_min['min_value']=cdn_dic['all']['band_width'];
                                        max_min['min_time']=time_temp;
                                    }
                                    for(var cdn_temp in cdn_dic)
                                    {
                                        detail=cdn_dic[cdn_temp];
                                        for(var data_type in detail)
                                        {
                                            if (!detail_agent_data[cdn_temp])
                                            {
                                                detail_agent_data[cdn_temp]={};
                                            }
                                            if (!detail_agent_data[cdn_temp][data_type])
                                            {
                                                detail_agent_data[cdn_temp][data_type]=0;
                                            }
                                            detail_agent_data[cdn_temp][data_type]+=parseInt(detail[data_type]);
                                        }
                                    }
                                    value_count+=1;
                                    if(value_count >=limit_count)
                                    {
                                        out_temp[value_first_time]={};
                                        for (cdn_temp in detail_agent_data)
                                        {
                                            detail_agent_data[cdn_temp]['band_width']=detail_agent_data[cdn_temp]['band_width']/limit_count;
                                            detail_agent_data[cdn_temp]['concurrent']=detail_agent_data[cdn_temp]['concurrent']/limit_count;
                                            detail_agent_data[cdn_temp]['edge_band_width']=detail_agent_data[cdn_temp]['edge_band_width']/limit_count;
                                            detail_agent_data[cdn_temp]['edge_concurrent']=detail_agent_data[cdn_temp]['edge_concurrent']/limit_count;
                                            // { liupan add, 2018/1/18
                                            detail_agent_data[cdn_temp]['edge_band_width-new'] = detail_agent_data[cdn_temp]['edge_band_width-new'] / limit_count;
                                            for (var at = 0; at < agent_types.length; ++at) {
                                                var agent = agent_types[at];

                                                if (agent in detail_agent_data[cdn_temp]) {
                                                    detail_agent_data[cdn_temp][agent] = detail_agent_data[cdn_temp][agent] / limit_count;
                                                }
                                                else {
                                                    detail_agent_data[cdn_temp][agent] = 0;
                                                }
                                            }
                                            // } liupan add, 2018/1/18
                                        }
                                        out_temp[value_first_time]=detail_agent_data;
                                        detail_agent_data={};
                                        value_count=0;
                                    }
                                }
                                if(value_count!=0)
                                {
                                    out_temp[value_first_time]={};
                                    for (cdn_temp in detail_agent_data)
                                    {
                                        detail_agent_data[cdn_temp]['band_width']=detail_agent_data[cdn_temp]['band_width']/value_count;
                                        detail_agent_data[cdn_temp]['concurrent']=detail_agent_data[cdn_temp]['concurrent']/value_count;
                                        detail_agent_data[cdn_temp]['edge_band_width']=detail_agent_data[cdn_temp]['edge_band_width']/value_count;
                                        detail_agent_data[cdn_temp]['edge_concurrent']=detail_agent_data[cdn_temp]['edge_concurrent']/value_count;
                                        // { liupan add, 2018/1/18
                                        detail_agent_data[cdn_temp]['edge_band_width-new'] = detail_agent_data[cdn_temp]['edge_band_width-new'] / value_count;
                                        for (var at = 0; at < agent_types.length; ++at) {
                                            var agent = agent_types[at];

                                            if (agent in detail_agent_data[cdn_temp]) {
                                                detail_agent_data[cdn_temp][agent] = detail_agent_data[cdn_temp][agent] / value_count;
                                            }
                                            else {
                                                detail_agent_data[cdn_temp][agent] = 0;
                                            }
                                        }
                                        // } liupan add, 2018/1/18
                                    }
                                    out_temp[value_first_time]=detail_agent_data;
                                    detail_agent_data={};
                                }
                                logger.info('value_count');
                                logger.info(value_first_time);

                                out_temp[max_min['max_time']]=struct_sum[max_min['max_time']];
                                out_temp[max_min['min_time']]=struct_sum[max_min['min_time']];
                                out_temp[last_time]=struct_sum[last_time];

                                for(var time_temp in out_temp) { 
                                    all_output[time_temp]={};
                                    cdn_dic=out_temp[time_temp]
                                    
                                    for (var cdn_temp in cdn_dic) {
                                        detail=cdn_dic[cdn_temp];
                                        all_output[time_temp][cdn_temp]={};
                                        all_output[time_temp][cdn_temp]['band_width']=detail['band_width'];
                                        all_output[time_temp][cdn_temp]['concurrent']=detail['concurrent']?parseInt(detail['concurrent']):0;
                                        all_output[time_temp][cdn_temp]['edge_band_width']=detail['edge_band_width'];
                                        // { liupan add, 2018/1/18
                                        all_output[time_temp][cdn_temp]['edge_band_width-new'] = detail['edge_band_width-new'];
                                        // } liupan add, 2018/1/18
                                        all_output[time_temp][cdn_temp]['edge_concurrent']=detail['edge_concurrent']?parseInt(detail['edge_concurrent']):0;
                                        // {   // 2017/6/23 by liupan
                                        // all_output[time_temp][cdn_temp]['android_hls_new_v1_rate']=detail['android-hls-new-v1']?detail['android-hls-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_hls_new_v2_rate']=detail['android-hls-new-v2']?detail['android-hls-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_hls_sum_rate']=detail['android-hls']?detail['android-hls']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_pzsp_new_v1_rate']=detail['android-pzsp-new-v1']?detail['android-pzsp-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_pzsp_new_v2_rate']=detail['android-pzsp-new-v2']?detail['android-pzsp-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_pzsp_sum_rate']=detail['android-pzsp']?detail['android-pzsp']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['ios_hls_new_v1_rate']=detail['ios-hls-new-v1']?detail['ios-hls-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['ios_hls_new_v2_rate']=detail['ios-hls-new-v2']?detail['ios-hls-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['ios_hls_sum_rate']=detail['ios-hls']?detail['ios-hls']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_flv_new_v1_rate']=detail['pc-flv-new-v1']?detail['pc-flv-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_flv_new_v2_rate']=detail['pc-flv-new-v2']?detail['pc-flv-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_flv_sum_rate']=detail['pc-flv']?detail['pc-flv']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hds_new_v1_rate']=detail['pc-hds-new-v1']?detail['pc-hds-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hds_new_v2_rate']=detail['pc-hds-new-v2']?detail['pc-hds-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hds_sum_rate']=detail['pc-hds']?detail['pc-hds']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hls_new_v1_rate']=detail['pc-hls-new-v1']?detail['pc-hls-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hls_new_v2_rate']=detail['pc-hls-new-v2']?detail['pc-hls-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hls_sum_rate']=detail['pc-hls']?detail['pc-hls']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['sum_new_v1_rate']=detail['agent_new_v1_sum']?detail['agent_new_v1_sum']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['sum_new_v2_rate']=detail['agent_new_v2_sum']?detail['agent_new_v2_sum']*100/detail['agent_sum']:0
                                        // } // 2017/6/23 by liupan
                                        // { 2017/6/23 by liupan
                                        // { liupan add, 2018/1/18
                                        all_output[time_temp][cdn_temp]['h5_sum_rate']=detail['h5']?detail['h5']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['h5_new_rate']=detail['h5-new']?detail['h5-new']*100/detail['agent_sum']:0
                                        // } liupan add, 2018/1/18
                                        all_output[time_temp][cdn_temp]['android_hls_sum_rate']=detail['android-hls']?detail['android-hls']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['android_hls_new_rate']=detail['android-hls-new']?detail['android-hls-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['android_pzsp_sum_rate']=detail['android-pzsp']?detail['android-pzsp']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['android_pzsp_new_rate']=detail['android-pzsp-new']?detail['android-pzsp-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['ios_hls_new_rate']=detail['ios-hls-new']?detail['ios-hls-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['ios_hls_sum_rate']=detail['ios-hls']?detail['ios-hls']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_flv_new_rate']=detail['pc-flv-new']?detail['pc-flv-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_flv_sum_rate']=detail['pc-flv']?detail['pc-flv']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hds_new_rate']=detail['pc-hds-new']?detail['pc-hds-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hds_sum_rate']=detail['pc-hds']?detail['pc-hds']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hls_new_rate']=detail['pc-hls-new']?detail['pc-hls-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hls_sum_rate']=detail['pc-hls']?detail['pc-hls']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['sum_new_rate']=detail['agent_new_sum']?detail['agent_new_sum']*100/detail['agent_sum']:0
                                        // }
                                        //all_output[time_temp][cdn_temp]['android']=detail['android']/detail['agent_sum']
                                        
                                        // { liupan add, 2018/1/18
                                        all_output[time_temp][cdn_temp]['h5_sum_rate_bw'] = detail['h5-bandwidth'] ? detail['h5-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['h5_new_rate_bw']=detail['h5-bandwidth-new']?detail['h5-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['android_hls_sum_rate_bw'] = detail['android-hls-bandwidth'] ? detail['android-hls-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['android_hls_new_rate_bw']=detail['android-hls-bandwidth-new']?detail['android-hls-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['android_pzsp_sum_rate_bw']=detail['android-pzsp-bandwidth']?detail['android-pzsp-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['android_pzsp_new_rate_bw']=detail['android-pzsp-bandwidth-new']?detail['android-pzsp-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['ios_hls_new_rate_bw']=detail['ios-hls-bandwidth-new']?detail['ios-hls-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['ios_hls_sum_rate_bw']=detail['ios-hls-bandwidth']?detail['ios-hls-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['pc_flv_new_rate_bw']=detail['pc-flv-bandwidth-new']?detail['pc-flv-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['pc_flv_sum_rate_bw']=detail['pc-flv-bandwidth']?detail['pc-flv-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['pc_hds_new_rate_bw']=detail['pc-hds-bandwidth-new']?detail['pc-hds-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['pc_hds_sum_rate_bw']=detail['pc-hds-bandwidth']?detail['pc-hds-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['pc_hls_new_rate_bw']=detail['pc-hls-bandwidth-new']?detail['pc-hls-bandwidth-new']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['pc_hls_sum_rate_bw']=detail['pc-hls-bandwidth']?detail['pc-hls-bandwidth']*100/detail['edge_band_width']:0
                                        all_output[time_temp][cdn_temp]['sum_new_rate_bw']=detail['edge_band_width-new']?detail['edge_band_width-new']*100/detail['edge_band_width']:0
                                        // } liupan add, 2018/1/18
                                    }
                                }
                                data_out.push(struct_sum);
                                data_out.push(all_output);
                                //console.log(struct_sum);
                                //res.json(all_output);
                                res.json(all_output);
                                db.close();
                                return;
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
// } liupan add, 2017/8/1

var get_user_sum_cdn_history_func = function(req,res){
    var output={};
    var kw_output={};
    var ws_output={};
    var dl_output={};
    var limit_count=LIMIT_COUNT*3
    try{
        connect_mongo(res,function(db){
                db.collection('history_user_sum',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=0;//req.body.cdn;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['region']=region_temp;
                        }
                        if (operator_temp!=0)
                        {
                            query['operator']=operator_temp;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        //console.log(query);
                        //type=req.body.history_type;

                        var back = {
                            "_id":0,
                            "time":1,
                            "band_width":1,
                            "edge_band_width":1,
                            "freeze_rate":1,
                            "bitrate":1,
                            "success_rate":1,
                            "agent":1,
                            //"region":1,
                            //"operator":1,
                            "cdn":1
                        }
                        //back[type]=1;
                        //console.log(back);
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json('err len(logs)=0');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};
                                var data_out=[];
                                var struct_sum={};
                                var struct_count={'all':{}};
                                for (var i=0; i <logs.length;i++)
                                {
                                    if(!logs[i]['agent'])
                                    {
                                        continue;
                                    }
                                    var data_temp=logs[i]['agent'];
                                    var time_temp=logs[i]['time'];
                                    if(!struct_sum[time_temp])
                                    {
                                        struct_sum[time_temp]={};
                                    }
                                    for(var cdn_temp in data_temp)
                                    {
                                        //console.log('cdn:',cdn_temp)
                                        if(!struct_sum[time_temp][cdn_temp])
                                        {
                                            struct_sum[time_temp][cdn_temp]={};
                                            struct_sum[time_temp][cdn_temp]['concurrent']=0;
                                            struct_sum[time_temp][cdn_temp]['edge_concurrent'] = 0;
                                        }
                                        struct_sum[time_temp][cdn_temp]['concurrent']+=data_temp[cdn_temp]['band_width']?data_temp[cdn_temp]['band_width']/data_temp[cdn_temp]['bitrate']:0;
                                        struct_sum[time_temp][cdn_temp]['edge_concurrent']+=data_temp[cdn_temp]['edge_band_width']?data_temp[cdn_temp]['edge_band_width']/data_temp[cdn_temp]['bitrate']:0;
                                        for(var detail in data_temp[cdn_temp])
                                        {
                                            if(!struct_sum[time_temp][cdn_temp][detail])
                                            {
                                                //console.log('new data'+detail);
                                                //console.log(data_temp[cdn_temp][detail]);
                                                struct_sum[time_temp][cdn_temp][detail]=0;
                                            }
                                            struct_sum[time_temp][cdn_temp][detail]+=data_temp[cdn_temp][detail];
                                        }
                                    }
                                }
                                var out_temp={}
                                var value_count=0;
                                var sum_interval_temp=0;
                                var detail_agent_data={};
                                var value_first_time=0;
                                var max_min={'max_time':0,'min_time':0,'max_value':0,'min_value':9999999}
                                var last_time=0;
                                var cdn_list=['kw','ws','dl'];
                                for(var time_temp in struct_sum)
                                {
                                    last_time=time_temp;
                                    cdn_dic=struct_sum[time_temp]
                                    var cdn_count=0;
                                    for(var cdn_temp in cdn_dic)
                                    {
                                        if(cdn_list.indexOf(cdn_temp)>=0)
                                        {
                                            cdn_count++;
                                        }
                                    }
                                    if(cdn_count<3)
                                    {
                                        continue;
                                    }
                                    if(value_count==0)
                                    {
                                        value_first_time=time_temp;
                                    }
                                    if(max_min['max_time']==0)
                                    {
                                        max_min['max_value']=cdn_dic['all']['band_width'];
                                        max_min['max_time']=time_temp;
                                        max_min['min_value']=cdn_dic['all']['band_width'];
                                        max_min['min_time']=time_temp;
                                    }
                                    if(cdn_dic['all']['band_width']>max_min['max_value'])
                                    {
                                        max_min['max_value']=cdn_dic['all']['band_width'];
                                        max_min['max_time']=time_temp;
                                    }
                                    if (cdn_dic['all']['band_width']<max_min['min_value'])
                                    {
                                        max_min['min_value']=cdn_dic['all']['band_width'];
                                        max_min['min_time']=time_temp;
                                    }
                                    for(var cdn_temp in cdn_dic)
                                    {
                                        detail=cdn_dic[cdn_temp];
                                        for(var data_type in detail)
                                        {
                                            if (!detail_agent_data[cdn_temp])
                                            {
                                                detail_agent_data[cdn_temp]={};
                                            }
                                            if (!detail_agent_data[cdn_temp][data_type])
                                            {
                                                detail_agent_data[cdn_temp][data_type]=0;
                                            }
                                            detail_agent_data[cdn_temp][data_type]+=parseInt(detail[data_type]);
                                        }
                                    }
                                    value_count+=1;
                                    if(value_count >=limit_count)
                                    {
                                        out_temp[value_first_time]={};
                                        for (cdn_temp in detail_agent_data)
                                        {
                                            detail_agent_data[cdn_temp]['band_width']=detail_agent_data[cdn_temp]['band_width']/limit_count;
                                            detail_agent_data[cdn_temp]['concurrent']=detail_agent_data[cdn_temp]['concurrent']/limit_count;
                                            detail_agent_data[cdn_temp]['edge_band_width']=detail_agent_data[cdn_temp]['edge_band_width']/limit_count;
                                            detail_agent_data[cdn_temp]['edge_concurrent']=detail_agent_data[cdn_temp]['edge_concurrent']/limit_count;
                                        }
                                        out_temp[value_first_time]=detail_agent_data;
                                        detail_agent_data={};
                                        value_count=0;
                                    }
                                }
                                if(value_count!=0)
                                {
                                    out_temp[value_first_time]={};
                                    for (cdn_temp in detail_agent_data)
                                    {
                                        detail_agent_data[cdn_temp]['band_width']=detail_agent_data[cdn_temp]['band_width']/value_count;
                                        detail_agent_data[cdn_temp]['concurrent']=detail_agent_data[cdn_temp]['concurrent']/value_count;
                                        detail_agent_data[cdn_temp]['edge_band_width']=detail_agent_data[cdn_temp]['edge_band_width']/value_count;
                                        detail_agent_data[cdn_temp]['edge_concurrent']=detail_agent_data[cdn_temp]['edge_concurrent']/value_count;
                                    }
                                    out_temp[value_first_time]=detail_agent_data;
                                    detail_agent_data={};
                                }
                                logger.info('value_count');
                                logger.info(value_first_time);

                                out_temp[max_min['max_time']]=struct_sum[max_min['max_time']];
                                out_temp[max_min['min_time']]=struct_sum[max_min['min_time']];
                                out_temp[last_time]=struct_sum[last_time];

                                for(var time_temp in out_temp)
                                {
                                    
                                    all_output[time_temp]={};
                                    cdn_dic=out_temp[time_temp]
                                    
                                    for(var cdn_temp in cdn_dic)
                                    {
                                        detail=cdn_dic[cdn_temp];
                                        all_output[time_temp][cdn_temp]={};
                                        all_output[time_temp][cdn_temp]['band_width']=detail['band_width'];
                                        all_output[time_temp][cdn_temp]['concurrent']=detail['concurrent']?parseInt(detail['concurrent']):0;
                                        all_output[time_temp][cdn_temp]['edge_band_width']=detail['edge_band_width'];
                                        all_output[time_temp][cdn_temp]['edge_concurrent']=detail['edge_concurrent']?parseInt(detail['edge_concurrent']):0;
                                        // {   // 2017/6/23 by liupan
                                        // all_output[time_temp][cdn_temp]['android_hls_new_v1_rate']=detail['android-hls-new-v1']?detail['android-hls-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_hls_new_v2_rate']=detail['android-hls-new-v2']?detail['android-hls-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_hls_sum_rate']=detail['android-hls']?detail['android-hls']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_pzsp_new_v1_rate']=detail['android-pzsp-new-v1']?detail['android-pzsp-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_pzsp_new_v2_rate']=detail['android-pzsp-new-v2']?detail['android-pzsp-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['android_pzsp_sum_rate']=detail['android-pzsp']?detail['android-pzsp']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['ios_hls_new_v1_rate']=detail['ios-hls-new-v1']?detail['ios-hls-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['ios_hls_new_v2_rate']=detail['ios-hls-new-v2']?detail['ios-hls-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['ios_hls_sum_rate']=detail['ios-hls']?detail['ios-hls']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_flv_new_v1_rate']=detail['pc-flv-new-v1']?detail['pc-flv-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_flv_new_v2_rate']=detail['pc-flv-new-v2']?detail['pc-flv-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_flv_sum_rate']=detail['pc-flv']?detail['pc-flv']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hds_new_v1_rate']=detail['pc-hds-new-v1']?detail['pc-hds-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hds_new_v2_rate']=detail['pc-hds-new-v2']?detail['pc-hds-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hds_sum_rate']=detail['pc-hds']?detail['pc-hds']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hls_new_v1_rate']=detail['pc-hls-new-v1']?detail['pc-hls-new-v1']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hls_new_v2_rate']=detail['pc-hls-new-v2']?detail['pc-hls-new-v2']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['pc_hls_sum_rate']=detail['pc-hls']?detail['pc-hls']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['sum_new_v1_rate']=detail['agent_new_v1_sum']?detail['agent_new_v1_sum']*100/detail['agent_sum']:0
                                        // all_output[time_temp][cdn_temp]['sum_new_v2_rate']=detail['agent_new_v2_sum']?detail['agent_new_v2_sum']*100/detail['agent_sum']:0
                                        // } // 2017/6/23 by liupan
                                        // { 2017/6/23 by liupan
                                        all_output[time_temp][cdn_temp]['android_hls_sum_rate']=detail['android-hls']?detail['android-hls']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['android_hls_new_rate']=detail['android-hls-new']?detail['android-hls-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['android_pzsp_sum_rate']=detail['android-pzsp']?detail['android-pzsp']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['android_pzsp_new_rate']=detail['android-pzsp-new']?detail['android-pzsp-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['ios_hls_new_rate']=detail['ios-hls-new']?detail['ios-hls-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['ios_hls_sum_rate']=detail['ios-hls']?detail['ios-hls']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_flv_new_rate']=detail['pc-flv-new']?detail['pc-flv-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_flv_sum_rate']=detail['pc-flv']?detail['pc-flv']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hds_new_rate']=detail['pc-hds-new']?detail['pc-hds-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hds_sum_rate']=detail['pc-hds']?detail['pc-hds']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hls_new_rate']=detail['pc-hls-new']?detail['pc-hls-new']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['pc_hls_sum_rate']=detail['pc-hls']?detail['pc-hls']*100/detail['agent_sum']:0
                                        all_output[time_temp][cdn_temp]['sum_new_rate']=detail['agent_new_sum']?detail['agent_new_sum']*100/detail['agent_sum']:0
                                        // }
                                        //all_output[time_temp][cdn_temp]['android']=detail['android']/detail['agent_sum']
                                    }
                                }
                                data_out.push(struct_sum);
                                data_out.push(all_output);
                                //console.log(struct_sum);
                                //res.json(all_output);
                                res.json(all_output);
                                db.close();
                                return;
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
};


var get_user_by_node_func = function(req,res){
    var output={};

    try{
        connect_mongo(res,function(db){
                db.collection('history_user_by_node',function(err,tb){
                    if(!err)
                    {
                        region_temp=req.body.region;
                        operator_temp=req.body.operator;
                        cdn_temp=req.body.cdn;
                        node_ip = req.body.node_ip;
                        // var channel_tmp=req.body.channel;
                        // console.log(channel_tmp);
                        var back = {
                            "_id":0,
                            "time":1,
                        }
                        var query =
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        if (region_temp!=0)
                        {
                            query['u_region']=region_temp;
                            back['u_region']=1;
                        }
                        if (operator_temp!=0)
                        {
                            query['u_operator']=operator_temp;
                            back['u_operator']=1;
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                            back['cdn']=1;
                        }
                        if (node_ip != 0){
                            query['s_ip'] = node_ip
                            back['s_ip'] = 1
                        }
                        // if(channel_tmp != 0)
                        // {
                        //     query['channel']=channel_tmp;
                        //     back['channel']=1;
                        // }
                        console.log(query);
                        type=req.body.history_type;

                        // { liupan add, 2018/3/9
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                            query['cdn'] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/9

                        back[type]=1;
                        // { liupan modify, 2017/8/18
                        // var max_min_value_dic={'max':-999999,'min':9999999999};
                        var max_min_value_dic = {};
                        if (type == "band_width") {
                            max_min_value_dic={'max':{'all':{'band_width':-999999},'sy':{'band_width':0}},'min':{'all':{'band_width':9999999999},'sy':{'band_width':0}}};
                        }
                        // { liupan add, 2018/6/19
                        else if (type == "ps_freeze_rate") {
                            max_min_value_dic = {'max':{'all':{'freeze_rate':-999999},'ps':{'freeze_rate':0}},'min':{'all':{'freeze_rate':9999999999},'ps':{'freeze_rate':0}}};
                        }
                        // } liupan add, 2018/6/19
                        else {
                            max_min_value_dic={'max':-999999,'min':9999999999};
                        }
                        // } liupan modify, 2017/8/18
                        var max_min_time_dic={'max':0,'min':0};
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {
                                    res.json('err');
                                    db.close();
                                    return;
                                }
                                var all_output={};
                                var out_count={};

                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    if (!(type in data_temp)) continue;
                                    if (output[data_temp["time"]])
                                    {
                                        {
                                            if (type == "band_width") {
                                                output[data_temp["time"]]["all"]["band_width"] += data_temp["band_width"];
                                                // if ("sy_band_width" in data_temp)
                                                //     output[data_temp["time"]]["sy"]["band_width"] += data_temp["sy_band_width"];
                                            }
                                            // { liupan add, 2018/6/19
                                            else if (type == "ps_freeze_rate") {
                                                output[data_temp["time"]]["all"]["freeze_rate"] += data_temp["freeze_rate"];
                                                if ("ps_freeze_rate" in data_temp)
                                                    output[data_temp["time"]]["ps"]["freeze_rate"] += data_temp["ps_freeze_rate"];
                                            }
                                            // } liupan add, 2018/6/19
                                            else {
                                                output[data_temp["time"]]+=data_temp[type];
                                            }
                                            // } liupan modify, 2017/8/18
                                            out_count[data_temp["time"]]+=1;
                                        }
                                    }
                                    else
                                    {
                                        // { liupan modify, 2017/8/18
                                        // output[data_temp["time"]]=data_temp[type];
                                        if (type == "band_width") {
                                            output[data_temp["time"]] = {"all":{"band_width":0}, "sy":{"band_width":0}};
                                            output[data_temp["time"]]["all"]["band_width"] = data_temp["band_width"];
                                            if ("sy_band_width" in data_temp)
                                                output[data_temp["time"]]["sy"]["band_width"] = data_temp["sy_band_width"];
                                            else
                                                output[data_temp["time"]]["sy"]["band_width"] = 0;
                                        }
                                        // { liupan add, 2018/6/19
                                        else if (type == "ps_freeze_rate") {
                                            output[data_temp["time"]] = {"all":{"freeze_rate":0}, "ps":{"freeze_rate":0}};
                                            output[data_temp["time"]]["all"]["freeze_rate"] = data_temp["freeze_rate"];
                                            if ("ps_freeze_rate" in data_temp)
                                                output[data_temp["time"]]["ps"]["freeze_rate"] = data_temp["ps_freeze_rate"];
                                            else
                                                output[data_temp["time"]]["ps"]["freeze_rate"] = 0;
                                        }
                                        // } liupan add, 2018/6/19
                                        else {
                                            output[data_temp["time"]]=data_temp[type];
                                        }
                                        // } liupan modify, 2017/8/18
                                        out_count[data_temp["time"]]=1;
                                    }
                                }
                                var band_arr=[];
                                // { liupan add, 2017/12/12
                                var band_4thDayPeak = {};   // 用于第四日峰值带宽统计方法：记录每一天的最大值
                                var band_4thDayPeak_arr = [];
                                // } liupan add, 2017/12/12
                                // { liupan add, 2017/8/18
                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                var sy_value_sum=0;
                                // } liupan add, 2017/8/18
                                if(type=='band_width')
                                {
                                    for(var t in output)
                                    {
                                        // { liupan modify, 2017/8/18
                                        // band_arr.push(output[t]);
                                        // var data_time_stamp=t;
                                        // var data_value=output[t];
                                        // if(data_value>max_min_value_dic['max'])
                                        // {
                                        //     max_min_value_dic['max']=data_value;
                                        //     max_min_time_dic['max']=data_time_stamp;
                                        // }
                                        // else if(data_value < max_min_value_dic['min'])
                                        // {
                                        //     max_min_value_dic['min']=data_value;
                                        //     max_min_time_dic['min']=data_time_stamp;
                                        // }
                                        band_arr.push(output[t]["all"]["band_width"]);
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["band_width"];
                                        if (data_value > max_min_value_dic['max']["all"]["band_width"]) {
                                            max_min_value_dic['max']["all"]["band_width"] = data_value;
                                            //max_min_value_dic['max']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        // { liupan modify, 2018/6/13
                                        // else if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                        if(data_value < max_min_value_dic['min']["all"]["band_width"]) {
                                        // } liupan modify, 2018/6/13
                                            max_min_value_dic['min']["all"]["band_width"] = data_value;
                                            //max_min_value_dic['min']["sy"]["band_width"] = output[t]["sy"]["band_width"];
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["band_width"];
                                        sy_value_sum += output[t]["sy"]["band_width"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};
                                            out_result[value_first_time]['all']['band_width'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['sy']['band_width'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                        // } liupan modify, 2017/8/18
                                        // { liupan add, 2017/12/12
                                        var date = new Date(t * 1000);
                                        var ymd = "" + date.getFullYear() + date.getMonth() + date.getDate();
                                        if (!(ymd in band_4thDayPeak)) {
                                            band_4thDayPeak[ymd] = output[t]["all"]["band_width"];
                                        }
                                        else {
                                            if (band_4thDayPeak[ymd] < output[t]["all"]["band_width"])
                                                band_4thDayPeak[ymd] = output[t]["all"]["band_width"];
                                        }
                                        // } liupan add, 2017/12/12
                                    }
                                    // { liupan add, 2018/6/13
                                    if (value_count != 0) {
                                        out_result[value_first_time] = {'all':{'band_width':0}, 'sy':{'band_width':0}};
                                        out_result[value_first_time]['all']['band_width'] = value_sum / value_count;
                                        out_result[value_first_time]['sy']['band_width'] = sy_value_sum / value_count;
                                    }
                                    // } liupan add, 2018/6/13

                                    // { liupan add, 2017/12/12
                                    for (var ymd in band_4thDayPeak) {
                                        band_4thDayPeak_arr.push(band_4thDayPeak[ymd]);
                                    }
                                    // } liupan add, 2017/12/12
                                }
                                // { liupan add, 2018/6/19
                                else if (type == 'ps_freeze_rate')
                                {
                                    for(var t in output)
                                    {
                                        if (out_count[t] != 0) {
                                            output[t] = output[t] / out_count[t];
                                        }
                                        var data_time_stamp = t;
                                        var data_value = output[t]["all"]["freeze_rate"];
                                        if (data_value > max_min_value_dic['max']["all"]["freeze_rate"]) {
                                            max_min_value_dic['max']["all"]["freeze_rate"] = data_value;
                                            max_min_time_dic['max'] = data_time_stamp;
                                        }
                                        if(data_value < max_min_value_dic['min']["all"]["freeze_rate"]) {
                                            max_min_value_dic['min']["all"]["freeze_rate"] = data_value;
                                            max_min_time_dic['min'] = data_time_stamp;
                                        }

                                        ori_count += 1;
                                        if(value_count == 0) {
                                            value_first_time = t;
                                        }
                                        value_count += 1;
                                        value_sum += output[t]["all"]["freeze_rate"];
                                        sy_value_sum += output[t]["ps"]["freeze_rate"];
                                        if (value_count >= LIMIT_COUNT) {
                                            out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};
                                            out_result[value_first_time]['all']['freeze_rate'] = value_sum / LIMIT_COUNT;
                                            out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / LIMIT_COUNT;
                                            test_count += 1;
                                            value_sum = 0;
                                            sy_value_sum = 0;
                                            value_count = 0;
                                        }
                                    }
                                    if (value_count != 0) {
                                        out_result[value_first_time] = {'all':{'freeze_rate':0}, 'ps':{'freeze_rate':0}};
                                        out_result[value_first_time]['all']['freeze_rate'] = value_sum / value_count;
                                        out_result[value_first_time]['ps']['freeze_rate'] = sy_value_sum / value_count;
                                    }
                                }
                                else
                                {
                                    for(var t in output)
                                    {
                                        if(out_count[t]!=0)
                                        {
                                            if (type != "user_n" && type != "req_n")
                                            output[t]=output[t]/out_count[t];
                                        }
                                        var data_time_stamp=t;
                                        var data_value=output[t];
                                        if(data_value>max_min_value_dic['max'])
                                        {
                                            max_min_value_dic['max']=data_value;
                                            max_min_time_dic['max']=data_time_stamp;
                                        }
                                        // { liupan modify, 2018/6/13
                                        // else if(data_value < max_min_value_dic['min'])
                                        if (data_value < max_min_value_dic['min'])
                                        // } liupan modify, 2018/6/13
                                        {
                                            max_min_value_dic['min']=data_value;
                                            max_min_time_dic['min']=data_time_stamp;
                                        }

                                        // { liupan add, 2017/8/18
                                        ori_count+=1;
                                        if(value_count==0)
                                        {
                                            value_first_time=t;
                                        }
                                        value_count+=1;
                                        value_sum+=output[t];
                                        if(value_count >=LIMIT_COUNT)
                                        {
                                            out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                            test_count+=1;
                                            value_sum=0;
                                            value_count=0;
                                        }
                                        // } liupan add, 2017/8/18
                                    }
                                    // { liupan add, 2018/6/13
                                    if (value_count != 0) {
                                        out_result[value_first_time] = value_sum / value_count;
                                    }
                                    // } liupan add, 2018/6/13
                                }
                                for (var xxx in out_result) {
                                    out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                    out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                    break;
                                }
                                // } liupan modify, 2018/6/13
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        // { liupan add, 2017/12/12
                                        var sort_b4dp = quickSort(band_4thDayPeak_arr);
                                        var band4dp = 0;
                                        if (sort_b4dp.length >= 4) {
                                            band4dp = sort_b4dp[sort_b4dp.length - 4];
                                        }
                                        else {
                                            band4dp = sort_b4dp[0];
                                        }
                                        all_output["detail"]=out_result;
                                        if (config.bandwidth_measurement_method == 0) {
                                            all_output["bandwidth95"]=band95;
                                        }
                                        else if (config.bandwidth_measurement_method == 1) {
                                            all_output["bandwidth4dp"] = band4dp;
                                        }
                                        break;
                                    case "bitrate":
                                    case "freeze_rate":
                                    case "success_rate":
                                    case "user_n":
                                    case "req_n":
                                    // { liupan add, 2018/6/19
                                    case "ps_freeze_rate":
                                    // } liupan add, 2018/6/19
                                    case "freeze_avg_iv":
                                    case "delayed_avg":
                                        all_output["detail"]=out_result;
                                        break;

                                    default:
                                        break;
                                    // } liupan modify, 2018/6/12
                                }
                                res.json(all_output);

                                db.close()
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close();
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })

        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
};


var get_channel_percent_func = function(req,res){
    var output={};
    var bit_replace={'0':'pd','1':'td','2':'ud','3':'hd','4':'md'};
    try{
        connect_mongo(res,function(db){
                db.collection('user_channel_table',function(err,tb){
                    if(!err)
                    {
                        var cdn_temp=req.body.cdn;
                        var channel_temp=req.body.channel;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        var back = {
                            "_id":0,
                            "time":1,
                            'cdn':1
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        if (channel_temp!=0)
                        {
                            var str_channel='channel.'+channel_temp;
                            back[str_channel]=1;
                        }
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json(logs);
                                    db.close();
                                    return;
                                }
                                var test_dic=[];
                                var all_output={};
                                var out_count={};
                                test_dic.push(logs);
                                var sum_bit_interval={};
                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    var time_temp=data_temp["time"];
                                    var channel_detail=data_temp['channel'][channel_temp]
                                    for( var index_bit in channel_detail)
                                    {
                                        if (output[time_temp])
                                        {
                                            output[time_temp][bit_replace[index_bit]]+=channel_detail[index_bit];
                                            sum_bit_interval[time_temp]+=channel_detail[index_bit];
                                        }
                                        else
                                        {
                                            output[time_temp]={'pd':0,'td':0,'ud':0,'md':0,'hd':0};
                                            output[time_temp][bit_replace[index_bit]]+=channel_detail[index_bit];
                                            sum_bit_interval[time_temp]=channel_detail[index_bit];
                                        }
                                    }
                                }
                                for (var time_stamp in output)
                                {
                                    all_output[time_stamp]={};
                                    for(var bit_temp in output[time_stamp])
                                    {
                                        all_output[time_stamp][bit_temp]=output[time_stamp][bit_temp]/sum_bit_interval[time_stamp];
                                    }
                                }

                                test_dic.push(output);
                                test_dic.push(sum_bit_interval);
                                test_dic.push(all_output);
                                res.json(all_output);
                                db.close();
                                return;

                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                for(var tt in output)
                                {
                                    ori_count+=1;
                                    if(value_count==0)
                                    {
                                        value_first_time=tt;
                                    }
                                    value_count+=1;
                                    value_sum+=output[tt];
                                    if(value_count >=LIMIT_COUNT)
                                    {
                                        out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                        test_count+=1;
                                        value_sum=0;
                                        value_count=0;
                                    }
                                }
                                out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                var band_arr=[];

                                var value_count=0;
                                var value_sum=0;
                                var value_first_time=0;
                                var out_result={};
                                var test_count=0;
                                var ori_count=0;
                                for(var tt in output)
                                {
                                    ori_count+=1;
                                    if(value_count==0)
                                    {
                                        value_first_time=tt;
                                    }
                                    value_count+=1;
                                    value_sum+=output[tt];
                                    if(value_count >=LIMIT_COUNT)
                                    {
                                        out_result[value_first_time]=value_sum/LIMIT_COUNT;
                                        test_count+=1;
                                        value_sum=0;
                                        value_count=0;
                                    }
                                }
                                out_result[max_min_time_dic['max']]=max_min_value_dic['max'];
                                out_result[max_min_time_dic['min']]=max_min_value_dic['min'];
                                test_out=[];
                                switch(type)
                                {
                                    case "band_width":
                                        var len=parseInt(band_arr.length*0.95)-1;
                                        var sort_result=quickSort(band_arr);
                                        var band95=sort_result[len];
                                        all_output["detail"]=out_result;
                                        all_output["bandwidth95"]=band95;
                                        break;
                                    case "bitrate":
                                        all_output["detail"]=out_result;
                                        break;
                                    case "freeze_rate":
                                        all_output["detail"]=out_result;
                                        break;
                                    case "success_rate":
                                        all_output["detail"]=out_result;
                                        break;
                                }
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

// { liupan add, 2017/8/1
var get_channel_percent_common_func = function(req,res){
    var output = {};
    var bit_replace = bitrate_type_list;            //{'0':'pd','1':'td','2':'ud','3':'hd','4':'md'};
    try{
        connect_mongo(res, function(db){
                db.collection('user_channel_table', function(err,tb){
                    if(!err)
                    {
                        var cdn_temp = req.body.cdn;
                        var channel_temp = req.body.channel;
                        var query = {
                            "time": {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        var back = {
                            "_id":0,
                            "time":1,
                            'cdn':1
                        }
                        if (cdn_temp != 0) {
                            query['cdn'] = cdn_temp;
                        }

                        // { liupan add, 2018/3/9
                        var cdn_list = [];
                        if (req.body.cdn_list != null && req.body.cdn_list != undefined && req.body.cdn_list != "none") {
                            cdn_list = req.body.cdn_list.split(",");
                            query['cdn'] = {$in: cdn_list};
                        }
                        // } liupan add, 2018/3/9

                        if (channel_temp != 0) {
                            var str_channel = 'channel.' + channel_temp;
                            back[str_channel] = 1;
                            // { liupan add, 2017/8/18
                            back[str_channel + '-sy'] = 1;
                            // } liupan add, 2017/8/18
                        }
                        tb.find(query, back).toArray(function(err, logs) {
                            if(!err) {
                                if (logs.length == 0) {
                                    res.json(logs);
                                    db.close();
                                    return;
                                }
                                var all_output = {};
                                var sum_bit_interval = {};
                                // { liupan add, 2017/8/18
                                var sy_sum_bit_interval = {};
                                // } liupan add, 2017/8/18
                                for (var i = 0; i < logs.length; i++) {
                                    var data_temp = logs[i];
                                    var time_temp = data_temp["time"];
                                    var channel_detail = data_temp['channel'][channel_temp];
                                    for( var index_bit in channel_detail) {
                                        // { liupan add, 2018/4/17
                                        if (!(index_bit in bit_replace)) continue;
                                        // } liupan add, 2018/4/17
                                        if (output[time_temp]) {
                                            // { liupan modify, 2017/8/18
                                            // output[time_temp][bit_replace[index_bit]] += channel_detail[index_bit];
                                            // sum_bit_interval[time_temp] += channel_detail[index_bit];
                                            output[time_temp]['all'][bit_replace[index_bit]] += channel_detail[index_bit];
                                            if ((channel_temp + '-sy') in data_temp['channel']) {
                                                output[time_temp]['sy'][bit_replace[index_bit]] += data_temp['channel'][channel_temp + '-sy'][index_bit];
                                                sy_sum_bit_interval[time_temp] += data_temp['channel'][channel_temp + '-sy'][index_bit];
                                            }
                                            sum_bit_interval[time_temp] += channel_detail[index_bit];
                                            // } liupan modify, 2017/8/18
                                        }
                                        else {
                                            // { liupan modify, 2017/8/18
                                            // output[time_temp] = {};     // {'pd':0,'td':0,'ud':0,'md':0,'hd':0};
                                            // for (var br in bit_replace) {
                                            //     output[time_temp][bit_replace[br]] = 0;
                                            // }
                                            // output[time_temp][bit_replace[index_bit]] += channel_detail[index_bit];
                                            // sum_bit_interval[time_temp] = channel_detail[index_bit];
                                            output[time_temp] = {'all':{}, 'sy':{}};
                                            for (var br in bit_replace) {
                                                output[time_temp]['all'][bit_replace[br]] = 0;
                                                output[time_temp]['sy'][bit_replace[br]] = 0;
                                            }
                                            output[time_temp]['all'][bit_replace[index_bit]] = channel_detail[index_bit];
                                            if ((channel_temp + '-sy') in data_temp['channel'])
                                                output[time_temp]['sy'][bit_replace[index_bit]] = data_temp['channel'][channel_temp + '-sy'][index_bit];
                                            else 
                                                output[time_temp]['sy'][bit_replace[index_bit]] = 0;
                                            sum_bit_interval[time_temp] = channel_detail[index_bit];
                                            sy_sum_bit_interval[time_temp] = output[time_temp]['sy'][bit_replace[index_bit]];
                                            // } liupan modify, 2017/8/18
                                        }
                                    }
                                }
                                var value_count = 0;
                                var sum_interval_temp = 0;
                                var channel_bit_temp = {};
                                // { liupan add, 2017/8/18
                                var channel_sy_bit_temp = {};
                                var sy_sum_interval_temp = 0;
                                // } liupan add, 2017/8/18
                                for (var tt in output) {
                                    if(value_count == 0) {
                                        value_first_time = tt;
                                    }
                                    // { liupan modify, 2017/8/18
                                    // for(var bit_temp in output[tt]) {
                                    //     if (!channel_bit_temp[bit_temp]) {
                                    //         channel_bit_temp[bit_temp] = 0;
                                    //     }
                                    //     channel_bit_temp[bit_temp] += output[tt][bit_temp];
                                    // }
                                    for (var bit_temp in output[tt]['all']) {
                                        if (!channel_bit_temp[bit_temp]) {
                                            channel_bit_temp[bit_temp] = 0;
                                        }
                                        channel_bit_temp[bit_temp] += output[tt]['all'][bit_temp];
                                    }
                                    for (var bit_temp in output[tt]['sy']) {
                                        if (!channel_sy_bit_temp[bit_temp]) {
                                            channel_sy_bit_temp[bit_temp] = 0;
                                        }
                                        channel_sy_bit_temp[bit_temp] += output[tt]['sy'][bit_temp];
                                    }
                                    sy_sum_interval_temp += sy_sum_bit_interval[tt];
                                    // } liupan modify, 2017/8/18
                                    sum_interval_temp += sum_bit_interval[tt];
                                    value_count += 1;
                                    if(value_count >= LIMIT_COUNT) {
                                        all_output[value_first_time] = {'all':{},'sy':{}};
                                        for(var bit_temp in channel_bit_temp) {
                                            // { liupan modify, 2017/8/18
                                            // all_output[value_first_time][bit_temp] = channel_bit_temp[bit_temp] / sum_interval_temp;
                                            all_output[value_first_time]['all'][bit_temp] = channel_bit_temp[bit_temp] / sum_interval_temp;
                                            // } liupan modify, 2017/8/18
                                        }
                                        // { liupan add, 2017/8/18
                                        for(var bit_temp in channel_sy_bit_temp) {
                                            if (sy_sum_interval_temp > 0)
                                                all_output[value_first_time]['sy'][bit_temp] = channel_sy_bit_temp[bit_temp] / sy_sum_interval_temp;
                                            else
                                                all_output[value_first_time]['sy'][bit_temp] = 0;
                                        }
                                        channel_sy_bit_temp = {};
                                        sy_sum_interval_temp = 0;
                                        // } liupan add, 2017/8/18
                                        sum_interval_temp = 0;
                                        channel_bit_temp = {};
                                        value_count = 0;
                                    }
                                }
                                res.json(all_output);
                                db.close();
                                return;
                            }
                            else {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}
// } liupan add, 2017/8/1

var get_CNTV_channel_percent_func2 = function(req,res){
    var output={};
    var bit_replace={'0':'pd','1':'td','2':'ud','3':'hd','4':'md'};
    try{
        connect_mongo(res,function(db){
                db.collection('user_channel_table',function(err,tb){
                    if(!err)
                    {
                        var cdn_temp=req.body.cdn;
                        var channel_temp=req.body.channel;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        var back = {
                            "_id":0,
                            "time":1,
                            'cdn':1
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        if (channel_temp!=0)
                        {
                            var str_channel='channel.'+channel_temp;
                            back[str_channel]=1;
                        }
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json(logs);
                                    db.close();
                                    return;
                                }
                                var test_dic=[];
                                var all_output={};
                                var out_count={};
                                test_dic.push(logs);
                                var sum_bit_interval={};
                                var out_result={};
                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    var time_temp=data_temp["time"];
                                    var channel_detail=data_temp['channel'][channel_temp]
                                    for( var index_bit in channel_detail)
                                    {
                                        if (output[time_temp])
                                        {
                                            output[time_temp][bit_replace[index_bit]]+=channel_detail[index_bit];
                                            sum_bit_interval[time_temp]+=channel_detail[index_bit];
                                        }
                                        else
                                        {
                                            output[time_temp]={'pd':0,'td':0,'ud':0,'md':0,'hd':0};
                                            output[time_temp][bit_replace[index_bit]]+=channel_detail[index_bit];
                                            sum_bit_interval[time_temp]=channel_detail[index_bit];
                                        }
                                    }
                                }
                                var value_count=0;
                                var sum_interval_temp=0;
                                var channel_bit_temp={};
                                for(var tt in output)
                                {
                                    if(value_count==0)
                                    {
                                        value_first_time=tt;
                                    }
                                    for(var bit_temp in output[tt])
                                    {
                                        if (!channel_bit_temp[bit_temp])
                                        {
                                            channel_bit_temp[bit_temp]=0;
                                        }
                                        channel_bit_temp[bit_temp]+=output[tt][bit_temp];
                                    }
                                    sum_interval_temp+=sum_bit_interval[tt];
                                    value_count+=1;
                                    if(value_count >=LIMIT_COUNT)
                                    {
                                        all_output[value_first_time]={};
                                        for(var bit_temp in channel_bit_temp)
                                        {
                                            all_output[value_first_time][bit_temp]=channel_bit_temp[bit_temp]/sum_interval_temp;
                                        }
                                        sum_interval_temp=0;
                                        channel_bit_temp={};
                                        value_count=0;
                                    }
                                }
                                res.json(all_output);
                                db.close();
                                return;
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
}

var get_ld_kw_channel_percent_func = function(req,res){
    var output={};
    var bit_replace={'0':'pd','1':'td','2':'ud','3':'hd','4':'md'};
    try{
        connect_mongo(res,function(db){
                db.collection('user_channel_table',function(err,tb){
                    if(!err)
                    {
                        var cdn_temp=req.body.cdn;
                        var channel_temp=req.body.channel;
                        var query = 
                        {
                            "time":
                            {
                                "$gt":node_history_start_time,
                                "$lt":node_history_end_time
                            }
                        }
                        var back = {
                            "_id":0,
                            "time":1,
                            'cdn':1
                        }
                        if (cdn_temp!=0)
                        {
                            query['cdn']=cdn_temp;
                        }
                        if (channel_temp!=0)
                        {
                            var str_channel='channel.'+channel_temp;
                            back[str_channel]=1;
                        }
                        tb.find(query,back).toArray(function(err,logs)
                        {
                            if(!err)
                            {
                                if ( logs.length==0 )
                                {   
                                    res.json(logs);
                                    db.close();
                                    return;
                                }
                                var test_dic=[];
                                var all_output={};
                                var out_count={};
                                test_dic.push(logs);
                                var sum_bit_interval={};
                                for (var i=0; i <logs.length;i++)
                                {
                                    var data_temp=logs[i];
                                    var time_temp=data_temp["time"];
                                    var channel_detail=data_temp['channel'][channel_temp]
                                    for( var index_bit in channel_detail)
                                    {
                                        if (output[time_temp])
                                        {
                                            if (!output[time_temp][index_bit])
                                            {
                                                output[time_temp][index_bit]=0;
                                            }
                                            output[time_temp][index_bit]+=channel_detail[index_bit];
                                            sum_bit_interval[time_temp]+=channel_detail[index_bit];
                                        }
                                        else
                                        {
                                            output[time_temp]={};
                                            if (!output[time_temp][index_bit])
                                            {
                                                output[time_temp][index_bit]=0;
                                            }
                                            output[time_temp][index_bit]+=channel_detail[index_bit];
                                            sum_bit_interval[time_temp]=channel_detail[index_bit];
                                        }
                                    }
                                }
                                for (var time_stamp in output)
                                {
                                    all_output[time_stamp]={};
                                    for(var bit_temp in output[time_stamp])
                                    {
                                        all_output[time_stamp][bit_temp]=output[time_stamp][bit_temp]/sum_bit_interval[time_stamp];
                                    }
                                }

                                test_dic.push(output);
                                test_dic.push(sum_bit_interval);
                                test_dic.push(all_output);
                                res.json(all_output);
                                db.close();
                                return;
                            }
                            else
                            {
                                db.close()
                                res.json({
                                    ErrNo:"102",
                                    ErrMsg:"Failed to get logs"
                                })
                            }
                        })
                    }
                    else
                    {
                        db.close()
                        res.json({
                            Err:err,
                            ErrNo:"101",
                            ErrMsg:"Failed to get table"
                        })
                    }
                })
            
        })
    }catch(e){
        res.json({
            ErrNo:"100",
            ErrMsg:"数据库错误"
        })
    }
};

exports.last_five = last_five
exports.complete = complete
exports.get_time = get_time
exports.test = test
exports.cdn_band = cdn_band
exports.connect_mongo = connect_mongo
exports.get_all_statistics_func=get_all_statistics_func
exports.get_region_statistics_func=get_region_statistics_func
exports.statistics_interface_func=statistics_interface_func
exports.history_interface_func=history_interface_func
// { liupan add, 2017/9/23
exports.getCurrentNodes = getCurrentNodes
exports.addIP2Local = addIP2Local
exports.deleteIPFromLocal = deleteIPFromLocal
// } liupan add, 2017/9/23
// { liupan add, 2017/9/26
exports.getCurrentNodes_IPList = getCurrentNodes_IPList
// } liupan add, 2017/9/26
