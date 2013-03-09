var Dimension = module.exports = function (data, keys, opts) {
	
	this.keys = keys;
	this._data = data;
	this.options = (typeof(opts) == 'object') ? opts : {};
	if (this.options.index !== false) this.createIndex();
	
};

Dimension.addToIndex = function (i,h,k,r) {
	if (typeof(i[h]) != 'object') {
		i[h] = {keys: k, rows : (Array.isArray(r)) ? r : [r]};
	} else if (!Array.isArray(r)) {
		i[h].rows.push(r);
	} else i[h].rows = i[h].rows.concat(r);
};

Dimension.prototype.createIndex = function () {
	var dim = this;
	this.index = {};
	var i = this._data.length;
	while(i--) {
		var k = {}, j = dim.keys.length, hash = '';
		while (j--) {
			hash += '\u0000' + dim._data[i][dim.keys[j]]; 
			k[dim.keys[j]] = (dim._data[i][dim.keys[j]] !== undefined) ? dim._data[i][dim.keys[j]] : null; 
		}
		dim.addToIndex(hash, k, i);
	}
};

Dimension.prototype.addToIndex = function (hash, keys, rownum) {
	Dimension.addToIndex(this.index, hash, keys, rownum);
};

Dimension.prototype.segment = function (s) {
	var segment = {}, iks = Object.keys(this.index), i = iks.length, sks = Object.keys(s);
	while (i--) {
		var j = sks.length, match = true;
		while (j--) {
			if (typeof(s[sks[j]]) == 'string' && this.index[iks[i]].keys[sks[j]] !== s[sks[j]]) {
				match = false;
				break;
			} else if (Array.isArray(s[sks[j]]) && s[sks[j]].indexOf(this.index[iks[i]].keys[sks[j]]) === -1) {
				match = false;
				break;
			}
		}
		if (match) segment[iks[i]] = this.index[iks[i]];
	}
	var nd = new Dimension(this._data, this.keys, true);
	nd.index = segment;
	return nd;
};

Dimension.prototype.reduce = function (drop) {
	if (!Array.isArray(drop)) drop = [drop];
	var keys = this.keys.filter(function (k) { return (drop.indexOf(k) === -1); });
	return this.aggregate(keys);
};

Dimension.prototype.aggregate = function (keys) {
	var segment = {} ; iks = Object.keys(this.index); i = iks.length;
	while (i--) {
		var j = keys.length, k = {}, hash = '';
		while (j--) {
			hash += '\u0000' + this.index[iks[i]].keys[keys[j]];
			k[keys[j]] = this.index[iks[i]].keys[keys[j]];
		}
		Dimension.addToIndex(segment, hash, k, this.index[iks[i]].rows);
	}
	var nd = new Dimension(this._data, keys, true);
	nd.index = segment;
	return nd;
};

Dimension.prototype.apply = function (f,r,b,m,s) {
	var dim = this, out = [];
	Object.keys(dim.index).forEach(function (k) { 
		var i = dim.index[k].rows.length, a = [], row = {};
		while (i--) a.push(dim._data[dim.index[k].rows[i]]);
	    Object.keys(dim.index[k].keys).forEach(function (kk) { row[kk] = dim.index[k].keys[kk]; });
	    row[f] = (typeof(m) == 'function') ? a.map(m).reduce(r, b) : a.reduce(r,b);
	    out.push(row);
	});
	if (typeof(s) == 'function') out.sort(s); 
	return out;
};

Dimension._hashKey = function (obj) {
	//return new Buffer(Object.keys(obj).map(function (k) {return obj[k];}).join('\u0000')).toString('base64');
	return JSON.stringify(obj);
};

Dimension._unhashKey = function (key) {
	return new Buffer(key, 'base64').toString('utf8').split('\u0000');
};