var util = require('util')
  , mongo = require('mongodb');

var speed = 1000
  , produce_rate = 5
  , timeout_rate = 5
  , pid = process.pid;

var format_ts = function(ts) {
  var dt = new Date(ts);
  return util.format('%d:%d:%d', dt.getHours(), dt.getMinutes(), dt.getSeconds());
};

var openCollection = function(callback) {
  new mongo.Db('lab'
    , new mongo.Server('localhost', 27017, {})
  ).open(function(err, client) {
    if (err) {
      console.log('Error when open database' + err);
    }
    callback(err, new mongo.Collection(client, 'msg_queue'));
  });
};

var counter = 0;

var produce = function() {
  counter ++;
  var doInsert = function(collection) {
    collection.insert(
      {
        number: counter
        , status: 'new'
        , message: 'Ticket ' + counter
        , ts: Date.now()
        , consumer: 0
      }
      , {safe: true}
      , function(err, objs) {
        if (err) {
          console.log('Error when insert ticket ' + counter);
        } else {
          objs.forEach(function(obj) {
            console.log(obj.message + ' Created by ' + pid + ' on ' + format_ts(obj.ts));
          });
        }
      }
    );
  };

  openCollection(function(err, collection) {
    if (collection) {
      doInsert(collection);
    }
  });
};

var consume = function(goodguy) {
  var doConsume = function(collection, id) {
    var ts = Date.now();
    collection.findAndModify(
      {_id: id, consumer: pid}
      , {}
      , {'$set': {status: 'taken', ts: ts}}
      , {}
      , function(err, obj) {
        if (err) {
          console.log('Error when taking ticket');
        } else if (obj) {
          console.log('Took ticket ' + obj.number + ' by ' + pid + ' on ' + format_ts(ts));
        } else {
          console.log('Ticket taken by someone else');
        }
      }
    );
  };

  var doGrab = function(collection) {
    var ts = Date.now();
    collection.findAndModify(
      {status: 'new'}
      , {ts: 1}
      , {'$set': {status: 'grabbed', ts: ts, consumer: pid}}
      , {}
      , function(err, obj) {
        if (err) {
          console.log('Error when grabbing ticket');
        } else if (obj) {
          var from = (obj.consumer in [0, pid])? '' : ' from ' + obj.consumer;
          console.log('Grabbed ticket ' + obj.number + ' by ' + pid + from + ' on ' + format_ts(ts));
          setTimeout(function() {
            doConsume(collection, obj._id);
          }, speed * (produce_rate - 1));
        } else {
          console.log('No ticket available');
        }
      }
    );
  };

  var doGC = function(collection) {
    var ts = Date.now();
    collection.findAndModify(
      {status: 'grabbed', ts: {'$lt': ts - speed * timeout_rate}}
      , {ts: 1}
      , {'$set': {status: 'grabbed', ts: ts, consumer: pid}}
      , {}
      , function(err, obj) {
        if (err) {
          console.log('Error when doing garbage collection');
        } else if (obj) {
          console.log('Grabbed ticket ' + obj.number + ' by ' + pid + ' from ' + obj.consumer + ' on ' + format_ts(ts));
          setTimeout(function() {
            doConsume(collection, obj._id);
          }, speed * (produce_rate - 1));
        } else {
          doGrab(collection);
        }
      }
    );
  };

  openCollection(function(err, collection) {
    if (collection) {
      if (goodguy) {
        doGC(collection);
      } else {
        doGrab(collection);
      }
    }
  });
};

var runProducer = function() {
  setInterval(
    produce
    , speed
  );
};

var runConsumer = function() {
  setInterval(
    consume
    , speed * produce_rate
  );
};

var runGoodGuy = function() {
  setInterval(
    function() {
      consume(true);
    }
    , speed * produce_rate
  );
}

var role = process.argv[2];

switch(role) {
  case 'producer':
    runProducer();
    break;
  case 'consumer':
    runConsumer();
    break;
  case 'goodguy':
    runGoodGuy();
    break;
  default:
    console.log('Run app by role producer/consumer/goodguy');
}
