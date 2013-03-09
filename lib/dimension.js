var Dimension = module.exports = function (data, keys, opts) {
	
	this.keys = keys;
	this._data = data;
	this.options = (typeof(opts) == 'object') ? opts : {};
	if (this.options.index !== false) this.createIndex(function () {});
	
};

Dimension.addToIndex = function (i,h,k,r) {
	if (typeof(i[h]) != 'object') {
		i[h] = {keys: k, rows : (Array.isArray(r)) ? r : [r]};
	} else if (!Array.isArray(r)) {
		i[h].rows.push(r);
	} else i[h].rows = i[h].rows.concat(r);
};

Dimension.indexKeyMatchesSegment = function (idx, segments, segkeys) {
	if (!segkeys) segkeys = Object.keys(segments);
	var j = segkeys.length, match = true;
	while (j--) {
		if (typeof(segments[segkeys[j]]) == 'string' && idx.keys[segkeys[j]] !== segments[segkeys[j]]) {
			match = false;
			break;
		} else if (Array.isArray(segments[segkeys[j]]) && segments[segkeys[j]].indexOf(idx.keys[segkeys[j]]) === -1) {
			match = false;
			break;
		}
	}
	return match;
};

Dimension.aggregateIndexKeyAsync = function (i, segment, idx, keys, cb) {
	Dimension.aggregateIndexKey(segment, idx, keys);
	process.nextTick(function () { cb(i); });
};

Dimension.aggregateIndexKey = function (segment, idx, keys) {
	var j = keys.length, k = {}, hash = '';
	while (j--) {
		hash += '\u0000' + idx.keys[keys[j]];
		k[keys[j]] = idx.keys[keys[j]];
	}
	Dimension.addToIndex(segment, hash, k, idx.rows);
};

Dimension.prototype.createIndex = function (cb) {
	this.index = {};
	var i = this._data.length - 1, dim = this;
	function next (n) { if (n--) { dim.indexRecordAsync(n, next); } else cb(); }
	
	this.indexRecordAsync(i, next);
};

Dimension.prototype.indexRecordAsync = function (i, cb) {
	this.indexRecord(i);
	process.nextTick(function () {cb(i);});
};

Dimension.prototype.indexRecord = function (i) {
	var k = {}, j = this.keys.length, hash = '';
	while (j--) {
		hash += '\u0000' + this._data[i][this.keys[j]]; 
		k[this.keys[j]] = (this._data[i][this.keys[j]] !== undefined) ? this._data[i][this.keys[j]] : null; 
	}
	this.addToIndex(hash, k, i);
};

Dimension.prototype.addToIndex = function (hash, keys, rownum) {
	Dimension.addToIndex(this.index, hash, keys, rownum);
};

Dimension.prototype.segment = function (s) {
	var segment = {}, iks = Object.keys(this.index), i = iks.length, sks = Object.keys(s);
	while (i--) if (Dimension.indexKeyMatchesSegment(this.index[iks[i]], s, sks)) segment[iks[i]] = this.index[iks[i]];
	var nd = new Dimension(this._data, this.keys, {index: false});
	nd.index = segment;
	return nd;
};

Dimension.prototype.segmentAsync = function (s, cb) {
	var segment = {}, iks = Object.keys(this.index), i = iks.length - 1, sks = Object.keys(s), dim = this;
	
	function next (n, result) {
		if (result === true) segment[iks[n]] = dim.index[iks[n]];
		if (n--) {
			dim.aSyncIndexKeyMatch(n, dim.index[iks[n]], s, sks, next);
		} else {
			var nd = new Dimension(dim._data, dim.keys, {index: false});
			nd.index = segment;
			cb(nd);
		}
	}
	
	this.aSyncIndexKeyMatch(i, dim.index[iks[i]], s, sks, next);
};

Dimension.prototype.aSyncIndexKeyMatch = function (i, idx, s, sks, cb) {
	var out = Dimension.indexKeyMatchesSegment(idx, s, sks);
	process.nextTick(function () { cb(i, out); });
};

Dimension.prototype.reduce = function (drop, cb) {
	if (!Array.isArray(drop)) drop = [drop];
	var keys = this.keys.filter(function (k) { return (drop.indexOf(k) === -1); });
	return this.aggregate(keys, cb);
};

Dimension.prototype.aggregate = function (keys, cb) {
	var segment = {} ; iks = Object.keys(this.index); i = iks.length - 1, dim = this;
	function next (n) {
		if (n--) {
			Dimension.aggregateIndexKeyAsync(n, segment, dim.index[iks[n]], keys, next);
		} else {
			var nd = new Dimension(dim._data, keys, {index: false});
			nd.index = segment;
			process.nextTick(function () {cb(nd);});
		}
	}
	Dimension.aggregateIndexKeyAsync(i, segment, this.index[iks[i]], keys, next);
};

Dimension.prototype.apply = function (f,r,b,m,s,cb) {
	var dim = this, out = [], iks = Object.keys(this.index), i = iks.length - 1;
	
	function next (n, result) {
		if (typeof(result) == 'object') out.push(result);
	    if (n--) {
	    	dim.applyToIndexSegmentAsync(n, dim.index[iks[n]], f, r, b, m, next);
	    } else {
	    	if (typeof(s) == 'function') out.sort(s); 
	    	cb(out);
	    }	
	}
	dim.applyToIndexSegmentAsync(i, dim.index[iks[i]], f, r, b, m, next);
};

Dimension.prototype.applyToIndexSegmentAsync = function (i, idx, f, r, b, m, cb) {
	var out = this.applyToIndexSegment(idx, f, r, b, m);
	process.nextTick(function () { cb(i, out); });
};

Dimension.prototype.applyToIndexSegment = function (idx,f,r,b,m) {
	var a = this.getSegment(idx.rows), row = {};
    Object.keys(idx.keys).forEach(function (k) { row[k] = idx.keys[k]; });
    row[f] = (typeof(m) == 'function') ? a.map(m).reduce(r, b) : a.reduce(r,b);
    return row;
};

Dimension.prototype.getSegment = function (rows) {
	var segment = [], i = rows.length;
	while (i--) segment.push(this._data[rows[i]]);
	return segment;
};
