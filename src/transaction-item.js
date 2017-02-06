import OptimisticLock from './optimistic-lock';
import ItemRef from './item-ref';
import {
    parseUpdateExpression,
    stringifyUpdateExpression,
    composeUpdateExpressions,
    defineExpressionAttributes,
    TX_OP,
    TX_ERROR,
    TX_IMAGE_TABLE_NAME
} from './util';

export default class TransactionItem {
    constructor(docClient, tx, itemRef, op, params) {
	const itemLock = new OptimisticLock(itemRef);
	const image_id = generateKey(itemRef);
	const imageLock = new OptimisticLock(new ItemRef(docClient, TX_IMAGE_TABLE_NAME, {
	    tx_id: tx.id,
	    image_id
	}));

	this._isTransient = false;
	this._isApplied = false;
	
	Object.defineProperties(this, {
	    _tx: {
		get() { return tx; }
	    },
	    _itemLock: {
		get() { return itemLock; }
	    },
	    _imageLock: {
		get() { return imageLock; }
	    },
	    operation: {
		enumerable: true,
		get() { return op; }
	    },
	    params: {
		enumerable: true,
		get() { return params; }
	    },
	    id: {
		enumerable: true,
		get() { return image_id; }
	    }
	});
    }

    _createTransientItem() {
	const txItem = this;
	const tx = txItem._tx;
	const transientItem = {
	    _tx_id: tx.id,
	    _tx_is_transient: true,
	    _tx_is_applied: false,
	    _tx_locked_at: new Date().toISOString()
	};
	
	return txItem._itemLock
	    .put({ Item: transientItem })
	    .then(() => txItem._isTransient = true);
    }

    _saveImage(image, timestamp) {
	const txItem = this;
	const tx = txItem._tx;

	return txItem._imageLock
	    .put({
		Item: {
		    image,
		    created_at: timestamp
		}
	    });
    }

    _lockItem(image) {
	const txItem = this;
	const tx = txItem._tx;
	const UpdateExpression = 'SET #_tx_id = :_tx_id, #_tx_locked_at = :_tx_locked_at, #_tx_is_applied = :_tx_is_applied';
	const now = new Date().toISOString();
	const {
	    ExpressionAttributeNames,
	    ExpressionAttributeValues
	} = defineExpressionAttributes({
	    '_tx_id': tx.id,
	    '_tx_locked_at': now,
	    '_tx_is_applied': false
	});
	const params = {
	    UpdateExpression,
	    ExpressionAttributeNames,
	    ExpressionAttributeValues
	};
	
	return txItem._itemLock
	    .update(params)
	    .then(() => txItem._saveImage(tx, image, now));
    }

    lock() {
	const txItem = this;
	const tx = this._tx;

	return txItem._itemLock.get()
	    .then(({ Item }) => {
				
		if (!Item) {
		    return txItem._createTransientItem(tx);
		} else if (!Item._tx_id) {
		    return txItem._lockItem(tx, Item);
		} else if (Item._tx_id !== tx.id) {
		    throw TX_ERROR.TX_LOCK_CONTENTION_ERROR ;
		} else {
		    throw TX_ERROR.TX_UNKNOWN_ERROR;
		}
	    });
    }


    unlock() {
	const txItem = this;

	if (txItem.operation === TX_OP.DELETE) {
	    return txItem._itemLock.delete();
	} else {
	    const UpdateExpression = 'REMOVE #_tx_id, #_tx_locked_at, #_tx_is_transient, #_tx_is_applied';
	    const params = {
		UpdateExpression,
		ExpressionAttributeNames: {
		    '#_tx_id': '_tx_id',
		    '#_tx_locked_at': '_tx_locked_at',
		    '#_tx_is_transient': '_tx_is_transient',
		    '#_tx_is_applied': '_tx_is_applied'
		}
	    };
	    
	    return txItem._itemLock.update(params);
	}
    }

    _applyPut() {
	const txItem = this;
	const tx = txItem._tx;
	const params = txItem.params;
	const Item = params.Item || {};
	
	return txItem._itemLock.put({
	    ...params,
	    Item: {
		...Item,
		_tx_id: tx.id,
		_tx_is_applied: true
	    }
	});
    }

    _applyUpdate() {
	const txItem = this;
	const tx = txItem._tx;
	const params = txItem.params;
	const UpdateExpression = composeUpdateExpressions(
	    params.UpdateExpression || '',
	    'SET #_tx_is_applied = :tx_is_applied'
	);
	const {
	    ExpressionAttributeNames,
	    ExpressionAttributeValues
	} = defineExpressionAttributes({
	    '_tx_is_applied': true
	});
	
	return txItem._itemLock.update({
	    ...(params || {}),
	    UpdateExpression,
	    ExpressionAttributeNames,
	    ExpressionAttributeValues
	});
    }

    apply() {
	const txItem = this;
	const op = txItem.operation;
	const deferred = Promise.defer();
	
	if (op === TX_OP.DELETE) {
	    deferred.resolve();
	} else if (op === TX_OP.PUT) {
	    txItem._applyPut()
		.then(() => deferred.resolve())
		.catch(err => deferred.reject(err));
	} else if (op == TX_OP.UPDATE) {
	    txItem._applyUpdate()
		.then(() => deferred.resolve())
		.catch(err => deferred.reject(err));
	} else if (op == TX_OP.GET) {
	    // TODO: Isolated reads
	} else {
	    deferred.reject(TX_ERROR.TX_UNSUPPORTED_OPERATION_ERROR);
	}

	return deferred.promise
	    .then(() => txItem._isApplied = true);
    }

    rollback() {
	const txItem = this;

	if (txItem._isTransient) {
	    return txItem._itemLock.delete();
	} else if (!txItem._isApplied) {
	    return txItem.unlock();
	} else {
	    return txItem._imageLock.get()
		.then(({ Item }) => {
		    if (!Item || !Item.image) {
			// TODO Handle error
		    }

		    return txItem._itemLock.put(Item.image);
		});
	}
    }
}

function generateKey(itemRef) {
    const keys = Object.keys(itemRef.key)
	      .sort()
	      .map(key => itemRef.key[key])
	      .join('_');
    return itemRef.tableName + '_' + keys;
}
