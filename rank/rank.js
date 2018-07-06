var logger = require('pomelo-logger').getLogger("rank", __filename);
var utils = require('../../util/utils');
var dao = require('../../dao/mongoose/mod/rankDao');
var mongoose = require('../../dao/mongoose');
var playerConst = require('../../consts/playerConst');
var redis = require('@tqnd/toolkit/lib/dao/redis');
var rankMode = playerConst.rankMode;

var MaxRankCount = 220;
var SaveInterval = 1000 * 60;
var SortInterval = 1000 * 60 * 60;

module.exports = function (app, detail, rankmode, cycle, mode, sub, cb) {
    var mod = new Mod(app, rankmode);
    mod.load(detail, mode, sub, cycle, cb);
};

var Mod = function(app, rankmode) {
    this.app = app;
    this.data = null;
    this.rankmode = rankmode;
    this.rankinfo = rankmode + "_info"
    this.shouldSave = false;
    this.change = true;
};

var pro = Mod.prototype;

pro.load = function(detail, mode, sub, cycle, cb) {
    var self = this;
    var model = dao.getRightModel(detail);

    if (!model) {
        cb("no valid model : "+ detail);
        return;
    }

    model.findByRankmode(cycle, mode, sub,function(err, data) {
        if (err) {
            cb(err);
            return;
        }

        if (!data) {
            var entity = new dao({ rankcycle : cycle, rankmode : mode, ranksubmode : sub, ranklist:[] });
            entity.save(function(err, data){
                if (err) {
                    cb(err);
                }
                else {
                    self.data = data;
                    self.minScore = calcMinRankScore(self.data.ranklist);
                    self.saveInterval();
                    self.rsort();
                    cb(null, self);
                }
            });
        } else {
            self.data = data;
            self.minScore = calcMinRankScore(self.data.ranklist);
            self.saveInterval();
            self.rsort();
            cb(null, self);
        }
    });
};

pro.getMinScore = function(){
    var self = this;
    return self.minScore || 0;
};

pro.processRankList = function(info){
    var self = this;
    if(!self.data || info.rankmode == undefined){
        return;
    }

    if(!self.data.ranklist){
        self.data.ranklist = [];
    }
    var newitem = {
        uid: info.uid,
        score: info.score,
        lasttime:new Date()
    };

    var idx = indexOfRankList(self.data.ranklist,newitem.uid);
    if(idx >= 0){
        if (self.data.ranklist[idx].score <= newitem.score) {
            return;
        }
        self.data.ranklist[idx].score = newitem.score;
        self.data.ranklist[idx].lasttime = newitem.lasttime;
        newitem._id = self.data.ranklist[idx]._id;
        self.raddrank(newitem);
    }
    else
    {
        if(newitem.score <= self.getMinScore()){
            return;
        }
        var len = self.data.ranklist.length;
        if(len < MaxRankCount){
            newitem._id = mongoose.generateId();
            self.data.ranklist.push(newitem);
            self.raddrank(newitem, true);
        }else{
            var tmpMin = calcMinRankScore(self.data.ranklist);
            if(newitem.score > tmpMin){
                var tmpindex = indexOfDelScore(self.data.ranklist,tmpMin);
                if(tmpindex >= 0){
                    newitem._id = mongoose.generateId();
                    var o_id = self.data.ranklist[tmpindex]._id;
                    self.data.ranklist.splice(tmpindex,1,newitem);
                    self.raddrank(newitem, true);
                    self.rdelrank(newitem._id);
                }else{
                    logger.info("==processRankListMonth error,cannot set in ranklist.info:%j",info);
                }
            }
        }
    }

    self.data.updatetime = new Date();
    self.shouldSave = true;
    self.change = true;

    self.minScore = calcMinRankScore(self.data.ranklist);
    logger.debug("=====end processRankListMonth minscore:"+self.minScore+ " mode:" + info.rankmode + "\ndata:%j",self.data);
};


pro.save = function(){
    if (this.data && this.shouldSave){
        this.shouldSave = false;
        this.data.save();
    }
};

pro.saveInterval = function(){
    var self = this;
    self.saveTimer = setInterval(function(){
        self.save();
    },10000);
};

pro.sortInterval = function(){
    var self = this;
    self.sortTimer = setTimeout(function(){
        self.sort();
    },SortInterval);
};

pro.destroy = function(){
    var self = this;
    self.save();

    if (self.saveTimer){
        clearInterval(self.saveTimer);
    }

    if (self.sortTimer) {
        clearInterval(self.sortInterval);
    }
};

pro.rsort = function() {
    var self = this;
    if (!self.data || self.change) {
        return;
    }

    var ranklistData = self.data.ranklist;
    var score = 0;
    var rankdata = [];
    var rankinfolist = [];
    for (var i = 0; i < ranklistData.length; ++i) {
        var rank = ranklistData[i];
        var score = rank.score;
        var _id = rank._id;
        rankdata.push(score);
        rankdata.push(_id);

        var rankInfo = JSON.stringify({uid : rank.uid, score : rank.score, name : rank.name, detailed : rank.detailed || {}});
        rankinfolist.push(_id);
        rankinfolist.push(rankInfo);
    }
    redis.rank.zadd(self.rankmode, rankdata);
    redis.rank.hset(self.rankinfo, rankinfolist);
};

pro.raddrank = function(item, updateinfo) {
    var self = this;
    redis.rank.zadd(self.rankmode, item.score, item._id, function(err, result) {
        if (err) {
            logger.err("raddrank zadd err:" + err);
        }
    });

    if (updateinfo) {
        var rankInfo = JSON.stringify({uid : item.uid, score : item.score, name : item.name, detailed : item.detailed || {}});
        redis.rank.hset(self.rankinfo, item._id, rankInfo, function(err, result) {
            if (err) {
                logger.err("raddrank hset err:" + err);
            }
        });
    }
};

pro.rdelrank = function(id) {
    var self = this;
    redis.rank.zrem(self.rankmode, id, function(err, result) {
        if (err) {
            logger.err("rdelrank zrem err:" + err);
        }
    });

    redis.rank.hdel(self.rankmode, id, function(err, result) {
        if (err) {
            logger.err("rdelrank hdel err:" + err);
        }
    });
};

pro.sort = function() {
    var self = this;
    if (!self.data || self.change) {
        return;
    }

    var ranklistData = self.data.ranklist;
    self.ranklist = {};
    var score = 0;
    for (var i = 0; i < ranklistData.length; ++i) {
        var rank = ranklistData[i];
        var score = rank.score;
        if (!self.ranklist[score]) {
            self.ranklist[score] = [];
        }
        self.ranklist[score].push({uid : rank.uid , name : rank.name, score : score});
    }

    var rankinfo = [];
    for (var k in self.ranklist) {
        rankinfo = rankinfo.concat(self.ranklist[k]);
    }

    redis.rank.hset("rank", self.rankmode, JSON.stringify(rankinfo));
    self.change = false;
    self.sortInterval();
};

var calcMinRankScore = function(ranklist){
    if(!ranklist || ranklist.length < MaxRankCount){
        return 0;
    }

    var minscore = ranklist[0].score || 0;
    for(var i = 1,len = ranklist.length; i < len; i++){
        var ts = ranklist[i].score || 0;
        if(ts < minscore){
            minscore = ts;
        }
    }
    return minscore;
};

var indexOfRankList = function(ranklist, uid){
    for(var i = 0; i < ranklist.length; i++){
        var tem = ranklist[i];
        if(tem.uid && tem.uid == uid){
            return i;
        }
    }
    return -1;
};

var indexOfDelScore = function(ranklist, score){
    if(!ranklist){
        return -1;
    }
    var retI = -1;
    var minLasttime = 0;
    for(var i= 0,len = ranklist.length;i<len;i++){
        if(!ranklist[i].lasttime){
            ranklist[i].lasttime = new Date();
        }
        if(ranklist[i].score == score){
            if(retI == -1){
                retI = i;
                minLasttime = ranklist[i].lasttime;
            }else{
                if(ranklist[i].lasttime > minLasttime){
                    retI = i;
                    minLasttime = ranklist[i].lasttime;
                }
            }
        }
    }
    return retI;
};
