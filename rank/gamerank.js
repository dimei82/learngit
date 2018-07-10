var logger = require('pomelo-logger').getLogger("rank", __filename);
var utils = require('../../util/utils');
var playerConst = require('../../consts/playerConst');
var redis = require('@tqnd/toolkit/lib/dao/redis');
var async = require("async");
var rankMode = playerConst.rankMode;

var APP = null;
var SID = null;
var DETAIL = "";
var ranklistContainer = {};
var updateInterval = 1000 * 60 * 60;

var pro = module.exports;

pro.init = function(app) {
    APP = app;
    SID = app.getServerId();

    if (SID) {
        var params = SID.split("-");
        if (params.length > 0) {
            DETAIL =  params.pop();
        }
    }
};

pro.getRanklistByMode = function(mode, sub, cb) {
    getRanklist(mode, sub, cb);
};

pro.getRanklist = function(mode, sub, page, pagesize, cb) {
    if (mode == 0 || page <= 0 || pagesize <= 0) {
        logger.error("getRanklist param err mode:%d sub:%d page:%d pagesize:%d", mode, sub, page, pagesize);
        cb(new Error("getRanklist err"));
        return;
    }
    getRanklist(mode, sub, function(err, res) {
        if (err) {
            logger.error("getRanklist err: "+ err);
            cb(err);
        }
        else {
            if (!res || res.length <= 0) {
                cb(null, []);
                return;
            }
            var begin = (page - 1) * pagesize;
            var end = page * pagesize;

            if (res.length < begin) {
                cb(null, []);
                return;
            }

            cb(null, res.splice(begin, end));
        }
    });
};

pro.getRanklistsByMode = function(modes, cb) {
    async.mapSeries(modes,
        function (mode, fn) {
            getRanklist(mode.mode, mode.submode, fn);
        },
        function (err, mods) {
            cb(err, mods);
        }
    );
};

var getRanklist = function(mode, submode, cb) {
    if (typeof mode !== "number") {
        return null;
    }

    var rank_mode = "rank" + "_" + DETAIL +  "_0" + "_" + mode + "_" + submode;
    var pr = ranklistContainer[rank_mode];

    if (!pr) {
        pr = ranklistContainer[rank_mode] = {};
        pr.callbacks = [];
    } else {
        var now = new Date();
        if (pr.validtime && now > pr.validtime) {
            pr.validtime = 0;
            if (pr.ranklist) {
                delete pr.ranklist;
            }
        }
    }

    var mod = pr.ranklist;
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

    rgetranklist(rank_mode, function(err, res) {
        if (!err) {
            pr.ranklist = res || [];
            pr.validtime = getDelayTime(updateInterval);
        }

        for (var i= 0, l = pr.callbacks.length; i<l; i++) {
            var fn = pr.callbacks[i];
            fn(err, pr.ranklist);
        }
        pr.callbacks = [];
        pr.run = false;
    });
};

var rgetranklist = function(rank_mode, cb) {
    var mode_info = rank_mode + "_info";
    var ranklist = [];
    async.waterfall([
        function(fn) {
            redis.rank.hgetall(mode_info, function(err, res){
                if (err) {
                    fn(err);
                    return;
                }
                var rankinfos = {};
                for (var k in res) {
                    rankinfos[k] = JSON.parse(res[k]);
                }

                fn(null, rankinfos);
            });
        }, function(rankinfos, fn) {
            redis.rank.zrevrange(rank_mode, 0, -1, "withscores", function(err, res){
                if (err){
                    fn(err);
                    return;
                }

                for (var i = 0; i < res.length;) {
                    if (rankinfos[res[i]]) {
                        var rank = rankinfos[res[i]];
                        if (rank.score) {
                            rank.score = Number(res[i+1]);
                        }
                        ranklist.push(rank);
                    }

                    i = i + 2;
                }
                fn(null);
            });
        }],
        function(err) {
            if (err) {
                logger.error("rgetranklist err : %s, rankmode : %s", err, rank_mode);
                cb(err);
            }
            else {
                cb(null, ranklist);
            }
        });

};

function getDelayTime(delayTime) {
    var now = Date.now();
    return now+ delayTime;
}

