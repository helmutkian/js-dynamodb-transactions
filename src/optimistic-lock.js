import ItemRef from './item-ref';

export default class OptimisticLock extends ItemRef {

    constructor(docClient, tableName, key) {
	super(docClient, tableName, key);

	this._version = null;
	
	Object.defineProperty(this, 'version', {
	    enumerable: true,
	    configurable: false,
	    get() {
		return this._version;
	    }
	});
    }
    
    get(params) {
	const lock = this;
	 
	return super.get({
	    ...(params || {}),
	    ConsistentRead: true
	})
	.then(result => {
	    lock._version = result.Item && result.Item._version ? parseInt(result.Item._version) : 0;
	    return result;
	});
    }

    update(params) {
	const lock = this;
	
	return lock._ensureVersion(params)
	    .then(() => {
		const { UpdateExpression } = params || {};
 		const {
		    ConditionExpression,
		    ExpressionAttributeValues,
		    ExpressionAttributeNames
		} = generateCondition(params, lock.version);
		
		return super.update({
		    ...(params || {}),
		    UpdateExpression: 'SET #_version = :_next_version ' + (UpdateExpression || ''),
		    ConditionExpression,
		    ExpressionAttributeNames,
		    ExpressionAttributeValues: {
			...ExpressionAttributeValues,
			':_next_version': lock.version + 1
		    }
		});
	    })
	    .then(result => {
		lock._version += 1;
		return result;
	    });
    }

    put(params) {
	const lock = this;
	
	return lock._ensureVersion()
	    .then(() => {
		const condition = generateCondition(params, lock.version);
		
		return super.put({
		    ...(params || {}),
		    ...condition,
		    Item: {
			...(params ? params.Item : {}),
			_version: lock.version + 1
		    }
	    })
	    .then(result => {
		lock._version += 1;
		return result;
	    });
	});
    }

    delete(params) {
	const lock = this;

	return lock._ensureVersion()
	    .then(() => {
		const condition = generateCondition(params, lock.version);
		
		return super.delete({
			...(params || {}),
			...condition
		});
	    })
	    .then(result => {
		this._version = null;
		return result;
	    });
    }


    _ensureVersion() {
	const lock = this;
	
	if (lock.version === null) {
	    return lock.get();
	} else {
	    return new Promise(resolve => resolve());
	}
    }
}

function generateCondition(params, version) {
    const { ConditionExpression, ExpressionAttributeValues, ExpressionAttributeNames } = params || {};
    const lockConditionClause = '(#_version = :_previous_version OR attribute_not_exists(#_version))'; 
    const updateConditionClause = ConditionExpression ? ' AND (' + ConditionExpression + ')' : '';
    
    return {
	ConditionExpression: lockConditionClause + updateConditionClause,
	ExpressionAttributeValues: { 
	    ...(ExpressionAttributeValues || {}), 
	    ':_previous_version': version
	},
	ExpressionAttributeNames: {
	    ...(ExpressionAttributeNames || {}),
	    '#_version': '_version'
	}
    };
}
