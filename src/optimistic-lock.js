import { composeUpdateExpressions } from './util';

export default class OptimisticLock {

    constructor(itemRef) {
	this._version = null;
	
	Object.defineProperties(this, {
	    version: {
		enumerable: true,
		configurable: false,
		get() {
		    return this._version;
		}
	    },
	    itemRef: {
		enumerable: true,
		configurable: false,
		get() {
		    return itemRef;
		}
	    }
	});
    }
    
    get(params) {
	const lock = this;
	 
	return lock.itemRef.get({
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
 		const {
		    ConditionExpression,
		    ExpressionAttributeValues,
		    ExpressionAttributeNames
		} = generateCondition(params, lock.version);
		const UpdateExpression = composeUpdateExpressions(
		    params.UpdateExpression || '',
		    'SET #_version = :_next_version'
		);
		return lock.itemRef.update({
		    ...(params || {}),
		    UpdateExpression,
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
		
		return lock.itemRef.put({
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
		
		return lock.itemRef.delete({
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

