import uuid from 'uuid';
import AWS from 'aws-sdk';
import chai from 'chai';
import ItemRef from '../dist/item-ref';
import OptimisticLock from '../dist/optimistic-lock';

const assert = chai.assert;

const config = new AWS.Config({
    region: 'us-west-2',
    endpoint: process.env.DYNAMO_TX_TEST_ENDPOINT || 'http://localhost:8008',
    accessKeyId: 'TEST_KEY_ID',
    secretAccessKey: 'TEST_SECRET_ACCESS_KEY'
});

const docClient = new AWS.DynamoDB.DocumentClient(config);
const dynamodb = new AWS.DynamoDB(config);

const TableName = 'Test';
const tableSchema = {
    TableName,
    KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
    AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
    ProvisionedThroughput: {
	ReadCapacityUnits: 1,
	WriteCapacityUnits: 1
    }
};

const STALE_LOCK_ERR = 'ConditionalCheckFailedException';

function getTestItem() {
    return {
	id: uuid.v4(),
	foo: 'bar'
    };
}

describe('OptimisticLock', () => {

    before(function (done) {
	const deferred = Promise.defer();
	
	this.timeout(30000);
	try {	    
	    dynamodb.listTables((err, data) => {
		if (err) {
		    deferred.reject(err);
		} else {
		    deferred.resolve(data);
		}
	    });
	} catch (err) {
	    done(err);
	}
	
	deferred.promise
	    .then(({ TableNames }) => {
		const deferred = Promise.defer();
		
		if (TableNames.indexOf('Test') < 0) {
		    return dynamodb.createTable(tableSchema, err => {
			if (err) {
			    deferred.reject(err);
			} else {
			    deferred.resolve();
			}
		    });
		} else {
		    deferred.resolve();
		}

		return deferred.promise;
	    })
	    .then(() => done())
	    .catch(err => done(err));
    });
    
    describe('#get', () => {
	it('should return the record', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lock = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const deferred = Promise.defer();

	    docClient.put({ TableName, Item: testItem })
		.promise()
		.then(() => lock.get())
	    	.then(({ Item }) => {
		    assert.deepEqual(Item, testItem);
		    done();
		})
		.catch(done);
	});
    });

    describe('#put', () => {
	it('should successfully create the item', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };	   
	    const lock = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const newItem = { ...testItem, foo: 'baz' };
	    
	    lock.put({ Item: newItem })
		.then(() => lock.get())
		.then(({ Item }) => {
		    assert.deepEqual(Item, { ...newItem, _version: 1 });
		    done();
		})
		.catch(done);
	});

	it('should fail to replace the item with a stale version', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lockA = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const lockB = new OptimisticLock(new ItemRef(docClient, TableName, Key));
 	    const newItemA = { ...testItem, foo: 'baz' };
	    const newItemB = { ...testItem, foo: 'quux' };

	    Promise.all([
		lockA.get(),
		lockB.get()
	    ])
		.then(() => lockA.put({ Item: newItemA }))
		.then(() => lockB.put({ Item: newItemB }))
		.then(() => {
		    assert.isOk(false);
		    done();
		})
		.catch(err => {
		    if (err.code === STALE_LOCK_ERR) {
			assert.isOk(true);
			done();
		    } else {
			done(err);
		    }
		});
	
	});

	it('should fail to replace with a stale version then succeed after refreshing the version', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lockA = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const lockB = new OptimisticLock(new ItemRef(docClient, TableName, Key));
 	    const newItemA = { ...testItem, foo: 'baz' };
	    const newItemB = { ...testItem, foo: 'quux' };

	    Promise.all([
		lockA.get(),
		lockB.get()
	    ])
	    .then(() => lockA.put({ Item: newItemA }))
            .then(() => lockB.put({ Item: newItemB }))
	    .then(() => {
		assert.isOk(false);
		done();
	    })
	    .catch(err => {
		if (err.code === STALE_LOCK_ERR) {
		    lockB.get()
			.then(() => lockB.put({ Item: newItemB }))
			.then(() => lockB.get())
			.then(({ Item }) => assert.deepEqual(Item, { ...newItemB, _version: 2 }))
			.then(() => done())
			.catch(done);
		} else {
		    done(err);
		}
	    });
	});
    });

    describe('#update', () => {
	it('should successfully update the item', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lock = new OptimisticLock(new ItemRef(docClient, TableName, Key));

	    docClient.put({ TableName, Item: testItem })
		.promise()
		.then(() => lock.update({
		    UpdateExpression: 'SET foo = :foo',
		    ExpressionAttributeValues: {
			':foo': 'baz'
		    },
		    ReturnValues: 'ALL_NEW'
		}))
		.then(({ Attributes }) => {
		    assert.deepEqual(Attributes, { ...testItem, foo: 'baz', _version: 1 });
		    done();
		})
		.catch(done);
	});

	it('should fail to update the item with a stale lock', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lockA = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const lockB = new OptimisticLock(new ItemRef(docClient, TableName, Key));

	    Promise.all([
		lockA.get(),
		lockB.get()
	    ])
	    .then(() => lockA.put({ Item: testItem }))
	    .then(() => lockB.update({
		UpdateExpression: 'SET foo = :foo',
		ExpressionAttributeValues: {
		    ':foo': 'baz'
		}
	    }))
	    .then(() => {
		assert.isOk(false);
		done();
	    })
	    .catch(err => {
		if (err.code === STALE_LOCK_ERR) {
		    assert.isOk(true);
		    done();
		} else {
		    done(err);
		}
	    });
	});

	it('should fail to update with a stale version then succeed after refreshing the version', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lockA = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const lockB = new OptimisticLock(new ItemRef(docClient, TableName, Key));

	    Promise.all([
		lockA.get(),
		lockB.get()
	    ])
	    .then(() => lockA.put({ Item: testItem }))
	    .then(() => lockB.update({
		UpdateExpression: 'SET foo = :foo',
		ExpressionAttributeValues: {
		    ':foo': 'baz'
		}
	    }))
	    .then(() => {
		assert.isOk(false);
		done();
	    })
	    .catch(err => {
		if (err.code === STALE_LOCK_ERR) {
		    lockB.get()
		    	.then(() => lockB.update({
			    UpdateExpression: 'SET foo = :foo',
			    ExpressionAttributeValues: {
				':foo': 'baz'
			    },
			    ReturnValues: 'ALL_NEW'
			}))
			.then(({ Attributes }) => {
			    assert.deepEqual(Attributes, { ...testItem, foo: 'baz', _version: 2 });
			    done();
			})
			.catch(done);
		} else {
		    done(err);
		}
	    }); 
	});
    });

    describe('#delete', () => {
	it('should successfully delete the item', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lock = new OptimisticLock(new ItemRef(docClient, TableName, Key));

	    docClient.put({ TableName, Item: testItem })
		.promise()
		.then(() => lock.delete({ ReturnValues: 'ALL_OLD' }))
		.then(({ Attributes }) => {
		    assert.deepEqual(Attributes, testItem);
		    done();
		})
		.catch(done);    
	});

	it('should fail to delete the item with a stale lock', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lockA = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const lockB = new OptimisticLock(new ItemRef(docClient, TableName, Key));

	    Promise.all([
		lockA.get(),
		lockB.get()
	    ])
	    .then(() => lockA.put({ Item: testItem }))
	    .then(() => lockB.delete())
	    .then(() => {
		assert.isOk(false);
		done();
	    })
	    .catch(err => {
		if (err.code === STALE_LOCK_ERR) {
		    assert.isOk(true);
		    done();
		} else {
		    done(err);
		}
	    });
	});

	it('should fail to delete with a stale version then succeed after refreshing the version', done => {
	    const testItem = getTestItem();
	    const Key = { id: testItem.id };
	    const lockA = new OptimisticLock(new ItemRef(docClient, TableName, Key));
	    const lockB = new OptimisticLock(new ItemRef(docClient, TableName, Key));

	    Promise.all([
		lockA.get(),
		lockB.get()
	    ])
	    .then(() => lockA.put({ Item: testItem }))
	    .then(() => lockB.delete())
	    .then(() => {
		assert.isOk(false);
		done();
	    })
	    .catch(err => {
		if (err.code === STALE_LOCK_ERR) {
		    lockB.get()
			.then(() => lockB.delete({ ReturnValues: 'ALL_OLD' }))
			.then(({ Attributes }) => {
			    assert.deepEqual(Attributes, { ...testItem, _version: 1 });
			    done();
			})
			.catch(done);
		} else {
		    done(err);
		}
	    });
	});

    });
});
