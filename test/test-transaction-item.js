import AWS from 'aws-sdk';
import chai from 'chai';
import uuid from 'uuid';
import TransactionItem from '../dist/transaction-item';
import ItemRef from '../dist/item-ref';
import { TX_OP, TX_IMAGE_TABLE_NAME, TX_ERROR } from '../dist/util';

const assert = chai.assert;

const config = new AWS.Config({
    region: 'us-west-2',
    endpoint: process.env.DYNAMO_TX_TEST_ENDPOINT || 'http://localhost:8008',
    accessKeyId: 'TEST_KEY_ID',
    secretAccessKey: 'TEST_SECRET_ACCESS_KEY'
});

const docClient = new AWS.DynamoDB.DocumentClient(config);
const dynamodb = new AWS.DynamoDB(config);



const tableSchemas = [
    {
	TableName: 'Test',
	KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
	AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
	ProvisionedThroughput: {
	    ReadCapacityUnits: 1,
	    WriteCapacityUnits: 1
	}
    },
    {
	TableName: TX_IMAGE_TABLE_NAME,
	KeySchema: [
	    { AttributeName: 'tx_id', KeyType: 'HASH' },
	    { AttributeName: 'image_id', KeyType: 'RANGE' }
	],
	AttributeDefinitions: [
	    { AttributeName: 'tx_id', AttributeType: 'S' },
	    { AttributeName: 'image_id', AttributeType: 'S' }
	],
	ProvisionedThroughput: {
	    ReadCapacityUnits: 1,
	    WriteCapacityUnits: 1
	}
    }
];

describe('TransactionItem', () => {

    before(function (done) {
	this.timeout(30000);

	ensureTable(tableSchemas)
	    .then(() => done())
	    .catch(done);
    });

    describe('#lock', () => {
	it('should lock the record to the transaction', done => {
	    const tx = createTx();
	    const itemRef = createItemRef();
	    const txItem = new TransactionItem(
		docClient,
		tx,
		itemRef,
		TX_OP.PUT,
		{ Item: { foo: 'foo' } }
	    );

	    itemRef.put()
		.then(() => txItem.lock())
		.then(() => itemRef.get())
		.then(({ Item: { _tx_id } }) => {
		    assert.equal(_tx_id, tx.id);
		    done();
		})
		.catch(done);
	});

	it('should save the image of a non-transient record', done => {
	    const tx = createTx();
	    const itemRef = createItemRef();
	    const txItem = new TransactionItem(
		docClient,
		tx,
		itemRef,
		TX_OP.PUT,
		{ Item: { foo: 'foo' } }
	    );
	    
	    itemRef.put({ Item: { foo: 'bar' } })
		.then(() => itemRef.get())
		.then(expected =>
		      txItem.lock()
		        .then(() => expected)
		)
		.then(expected =>
		      txItem._imageLock.get()
		        .then(actual => ([actual, expected]))
		)
		.then(([{ Item: { tx_id, image_id, image } }, { Item: expectedItem }]) => {

		    assert.deepEqual(
			{
			    tx_id,
			    image_id,
			    image
			},
			{
			    tx_id: tx.id,
			    image_id: txItem.id,
			    image: expectedItem
			}
		    );
		    done();
		})
		.catch(done);

	});

	
	it('should create a transient record locked to the transaction', done => {
	    const tx = createTx();
	    const itemRef = createItemRef();
	    const txItem = new TransactionItem(
		docClient,
		tx,
		itemRef,
		TX_OP.PUT,
		{ Item: { foo: 'foo' } }
	    );

	    txItem.lock()
		.then(() => itemRef.get())
		.then(({ Item: { _tx_id, _tx_is_transient } }) => {
		    assert.deepEqual({ _tx_id, _tx_is_transient }, { _tx_id: tx.id, _tx_is_transient: true });
		    done();
		})
		.catch(done);
	});

	it('should detect lock contention if a record is locked to another transaction', done => {
	    const tx1 = createTx();  
	    const tx2 = createTx();
	    const itemRef = createItemRef();
	    const txItem1 = new TransactionItem(
		docClient,
		tx1,
		itemRef,
		TX_OP.PUT,
		{ Item: { foo: 'foo' } }
	    );
	    const txItem2 = new TransactionItem(
		docClient,
		tx2,
		itemRef,
		TX_OP.PUT,
		{ Item: { foo: 'bar' } }
	    );

	    txItem1.lock()
		.then(() => txItem2.lock())
		.then(() => {
		    assert.isOk(false);
		    done();
		})
		.catch(err => {
		    if (err === TX_ERROR.TX_LOCK_CONTENTION_ERROR) {
			assert.isOk(true);
			done();
		    } else {
			done(err);
		    }
		});
	});

    });

    /*
    describe('#unlock', () => {
    });
    */

    
    describe('#apply', () => {
	
	it('should apply the PUT and mark the record as applied', done => {
	    const tx = createTx();
	    const itemRef = createItemRef();
	    const txItem = new TransactionItem(
		docClient,
		tx,
		itemRef,
		TX_OP.PUT,
		{ Item: { foo: 'foo' } }
	    );

	    txItem.lock()
		.then(() => txItem.apply())
		.then(() => itemRef.get())
		.then(({ Item: { foo, _tx_id, _tx_is_applied } }) => {
		    assert.deepEqual(
			{ foo, _tx_id, _tx_is_applied },
			{ foo: 'foo', _tx_id: tx.id, _tx_is_applied: true }
		    );
		    done();
		})
		.catch(done);
	});

	
	it('should apply the UPDATE and mark the record as applied', done => {
	    const tx = createTx();
	    const itemRef = createItemRef();
	    const txItem = new TransactionItem(
		docClient,
		tx,
		itemRef,
		TX_OP.UPDATE,
		{
		    UpdateExpression: 'SET foo = :foo',
		    ExpressionAttributeValues: {
			':foo': 'foo'
		    }
		}
	    );

	    txItem.lock()
		.then(() => txItem.apply())
		.then(() => itemRef.get())
		.then(({ Item: { foo, _tx_id, _tx_is_applied } }) => {
		    assert.deepEqual(
			{ foo, _tx_id, _tx_is_applied },
			{ foo: 'foo', _tx_id: tx.id, _tx_is_applied: true }
		    );
		    done();
		})
		.catch(done);

	});

	
	it('should not apply the DELETE or mark it as applied', done => {
	    const tx = createTx();
	    const itemRef = createItemRef();
	    const txItem = new TransactionItem(
		docClient,
		tx,
		itemRef,
		TX_OP.DELETE,
		{}
	    );

	    itemRef.put()
		.then(() => txItem.lock())
		.then(() => txItem.apply())
		.then(() => itemRef.get())
		.then(({ Item }) => {
		    if (!Item) {
			assert.isOk(false);
		    } else {
			const { _tx_id, _tx_is_applied } = Item;

			assert.deepEqual(
			    { _tx_id, _tx_is_applied },
			    { _tx_id: tx.id, _tx_is_applied: false }
			);
		    }
		    done();
		})
		.catch(done);
	});
    });

    /*
    describe('#rollback', () => {
	it('should delete the transient record', done => {
	});

	it('should unlock if changes are unapplied', done => {
	});

	it('should restore previously saved image', done => {
	});
    });
    */
});


function ensureTable(tableSchemas) {
    const deferred = Promise.defer();
    
    try {
	dynamodb.listTables((err, data) => {
	    if (err) {
		deferred.reject(err);
	    } else {
		deferred.resolve(data);
	    }
	});
    } catch (err) {
	deferred.reject(err);
    }

    return deferred.promise
	.then(({ TableNames }) => {
	    return Promise.all(
		tableSchemas
		    .filter(({ TableName })=> TableNames.indexOf(TableName) < 0)
		    .map(tableSchema => createTable(tableSchema))
	    );
	});
}

function createTable(tableSchema) {
    const deferred = Promise.defer();

    dynamodb.createTable(tableSchema, err => {
	if (err) {
	    deferred.reject(err);
	} else {
	    deferred.resolve();
	}
    });
}

function createTx() {
    return { id: uuid.v4() };
}

function createItemRef() {
    return new ItemRef(docClient, 'Test', { id: uuid.v4() });
}
