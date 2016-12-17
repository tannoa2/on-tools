var di = require('di');
var core = require('on-core')(di);
var injector = new di.Injector(core.injectables);
var waterline = injector.get('Services.Waterline');
var waterlineProtocol = injector.get('Protocol.Waterline');
var _ = require('lodash');
var Promise = injector.get('Promise');
var exec = require('child_process').exec;
var encryption = injector.get('Services.Encryption');
var chance = require('chance')

// Override waterline message publish with no-op.
waterlineProtocol.publishRecord = function () {
    return Promise.resolve();
};
var waterline_graphobjects_findAndModifyMongoAVG =0
var waterline_graphobjects_findAndModifyMongoARR = []
var sum =0;
var dbOperation = 'waterline.graphobjects.findMongo'
var typeT =  "findAndModify"
var  objsArr
var my_chance = new chance();


exec('mongodump', function(error, stdout, stderr) { // Backup mongo
 
    return encryption.start()
        .then (function(){
             return waterline.start();
        })
        .then(function(){
            if (dbOperation === 'waterline.graphobjects.findMongo') {
                return waterline.graphobjects.findMongo();
            }
        })
        .then(function(objs){
            console.log("Number of elments in the database: "+ objs.length)
            var t =
            objsArr = _.slice(objs,0,objs.length)//2392
            objsArr = _.slice(objs,0,2500)//2392
            return objsArr
        })
        .then(function(objs){
			//console.log(objs)
            var r
            var rr = []
            var objs1 =objs
            _.forEach(objs,function(element){

                if (dbOperation === 'waterline.graphobjects.findMongo') {
                    r = waterline.graphobjects.destroyOneById(element._id)
                        .then(function (r) {
                            return r
                        })
                }
                rr.push(r)
            })

            return Promise.all(rr)
            //return objs1
        })
        .then(function (r) {
            objsArr
            var options = {
                new: true,
                upsert: true,
                fields: {
                    _id: 0,
                    instanceId: 1
                }
            };
            return Promise.each(objsArr, function (element) {
                element.domain = element.domain.split('.')[0]
				element.domain = element.domain + '.' +my_chance.zip()
				//console.log("before: "+ element.domain)
                var start = 0;                
                //console.time('waterline_graphobjects_findAndModifyMongo')
                start = new Date().getTime()
                if (dbOperation === 'waterline.graphobjects.findMongo') {
                    var graph = element
                    var query = {
                        instanceId: graph.instanceId
                    };
                    return  waterline.graphobjects.findAndModifyMongo(query, {}, graph, options)
                        .then(function (data) {
                            //console.log(data)
                            var end = new Date().getTime()
                            //console.timeEnd('waterline_graphobjects_findAndModifyMongo')
                            var e = end - start
                            sum = sum + e
                            waterline_graphobjects_findAndModifyMongoARR.push(e)
                            return data
                        })
                }

                })
        })
        .then(function (r) {
            waterline_graphobjects_findAndModifyMongoAVG = sum / waterline_graphobjects_findAndModifyMongoARR.length
            console.log("Length: " + waterline_graphobjects_findAndModifyMongoARR.length)
            console.log("time_array of waterline_graphobjects_findAndModifyMongoARR: " + waterline_graphobjects_findAndModifyMongoARR + ' ms')
            console.log("time_avg of waterline_graphobjects_findAndModifyMongoAVG: " + waterline_graphobjects_findAndModifyMongoAVG + ' ms')
			return 1
            //waterline.stop();
        });
    return 1
});
