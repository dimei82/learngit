var logger = require('pomelo-logger').getLogger("rank", __filename);
var utils = require('../../util/utils');
var dao = require('../../dao/mongoose/mod/rankDao');
var serverCfg = require('../../../config/servers.json');
var moderankFunc = require('./rank');
var playerConst = require('../../consts/playerConst');
var rankroute = require("../../util/rankroute");
var async = require("async")
var rankMode = playerConst.rankMode;

var ranklistContainer = {};
//var rankcycle = playerConst.rankcycle;

var pro = module.exports;
var APP = null;
var SID = null;

pro.init = function(app, cb) {
   APP = app;
   SID = app.getServerId();

   this.load_bygame(cb);
};

pro.load_bygame = function(cb) {
    var self = this;
    var servers = APP.getServersByType("game");

    if (!servers) {
        servers = serverCfg[APP.env].rank || [];
    }

    for (var i = 0; i < servers.length; ++i) {
        var server = servers[i];
        if (server && server.id) {
            var sid = rankroute.routeRankByGame(server.id);
            if (SID == sid) {
                self.load_bygame_db(getDetail(server.id), 0, cb);
            }
        }
    }
};

pro.load_bygame_db = function(detail, cycle, cb) {
    var self = this;
    var model = dao.getRightModel(detail);

    if (!model) {
        cb("no valid model : "+ detail);
        return;
    }

    model.findbyCycleKey(cycle, function(err, datas) {
        if (err) {
            return;
        }
        if (datas.length <= 0) {
            return;
        }

        async.forEachLimit(datas, 5, function(rankData, callback) {
            var mode = rankData.rankmode;
            var c = rankData.rankcycle || 0;
            var sub = rankData.ranksubmode;

            self.getRankListMod(detail, c, mode, sub, function(err, mod){
                if (err || !mod) {
                    logger.error("get info mod err for uid:%d", self.uid);
                }
                callback(null);
            });
        }, function(err) {
            if (cb)  {
                cb(err);
            }
        });
    });
};


var load = function(detail, cycle, cb) {
    var self = this;
    var model = dao.getRightModel(detail);

    if (!model) {
        cb("no valid model : "+ detail);
        return;
    }

    model.findAllKey(cycle, function(err, datas) {
        if (err) {
            return;
        }
        if (datas.length <= 0) {
            return;
        }

        async.forEachLimit(datas, 5, function(rankData, callback) {
            var mode = rankData.rankmode;
            var c = rankData.rankcycle;
            var sub = rankData.ranksubmode;

            if (rankroute.routeRankByModeCycle(rankData.rankmode, c, sub) === SID) {
                self.getRankListMod(rankData.rankmode, detail, c, function(err, mod){
                    if (err || !mod) {
                        logger.error("get info mod err for uid:%d", self.uid);
                    }
                    else {
                        var rank_mode = detail + "_" + c + "_" + mode + "_" + sub;
                        var pr = ranklistContainer[rank_mode];

                        pr.module = mod;
                        pr.run = false;
                    }
                    callback(null);
                });
            }
        }, function(err) {
            if (cb)  {
                cb(err);
            }
        });
    });
};

pro.processRankInfoUpdate = function(params) {
    var mode = params.rankmode;
    var sub = params.submode;
    var cycle = params.cycle || 0;
    var detailname = getDetail(params.gamename);
    var rankinfo = params.rankinfo || [];

    if (!mode || !rankinfo) {
        logger.error("processRankInfoUpdate param err : %j", params);
        return;
    }

    var self = this;
    pro.getRankListMod(detailname, cycle, mode, sub, function(err, mod) {
        mod.processRankList(rankinfo);
    });
};

pro.getRankListMod = function(detailname, c, mode, sub, cb) {
    var rank_mode = "rank_" + detailname + "_" + c + "_" + mode + "_" + sub;
    var pr = ranklistContainer[rank_mode];

    if (!pr) {
        pr = ranklistContainer[rank_mode] = {};
        pr.callbacks = [];
    }

    var mod = pr.module;
    if (mod) {
        cb(null, mod);
        return;
    }

    pr.callbacks.push(cb);
    if (pr.callbacks.length > 1) {
        if (!pr.run) {
            logger.warn("getRankListMod err, have callbacks cache but not run");
        }else {
            return;
        }
    }
    pr.run = true;
    moderankFunc(APP, detailname, rank_mode, c, mode, sub, function (err, res) {
        if (!err){
            pr.module = res;
        }
        for (var i= 0, l = pr.callbacks.length; i<l; i++) {
            var fn = pr.callbacks[i];
            fn(err, res);
        }
        pr.callbacks = [];
        pr.run = false;
    });
};

function getDelayTime(delayTime) {
    var now = Date.now();
    return now+ delayTime;
}

function getDetail(sid) {
    var params = sid.split("-");
    if (params.length > 0) {
        return params.pop();
    }

    return "";
};