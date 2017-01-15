export default class ItemRef {
    constructor(docClient, tableName, key) {
	this._docClient = docClient;
	
	// Define immutable properties
	Object.defineProperties(this, {
	    tableName: {
		enumerable: true,
		configurable: false,
		get() { return tableName; }
	    },
	    key: {
		enumerable: true,
		configurable: false,
		get() { return key; }
	    }
	});
    }

    get(params) {
	const item = this;
	
	return item._docClient.get({
	    ...(params || {}),
	    TableName: item.tableName,
	    Key: item.key
	}).promise();
    }

    put(params) {
	const item = this;

	return item._docClient.put({
	    ...(params || {}),
	    TableName: item.tableName,
	    Item: {
		...(params ? params.Item : {}),
		...item.key
	    }
	}).promise(); 
    }

    update(params) {
	const item = this;

	return item._docClient.update({
	    ...(params || {}),
	    TableName: item.tableName,
	    Key: item.key
	}).promise();
    }

    delete(params) {
	const item = this;

	return item._docClient.delete({
	    ...(params || {}),
	    TableName: item.tableName,
	    Key: item.key
	}).promise();
    }
}
