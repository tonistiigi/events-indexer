
function Indexer() {

}

Indexer.prototype.set = function (key, property, value) {

}

Indexer.prototype.merge = function (key, object) {

}

Indexer.prototype.get = function (key) {

}

Indexer.prototype.range = function (start, end) {

}

Indexer.prototype.subscribe = function (start, end) {

}

Indexer.prototype.reducer = function (property, func) {

}

function Reducer() {

}

Reducer.prototype.set = function (key, id, value) {
  
}


module.exports = exports = function(opt) {
  return new Indexer(opt)
}

exports.Indexer = Indexer
exports.Reducer = Reducer