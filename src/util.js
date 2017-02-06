const UPDATE_EXPRESSION_TOPICS = ['SET', 'ADD', 'REMOVE', 'DELETE'];

export function parseUpdateExpression(updateExpression) {
    return updateExpression.split(/\s+/)
	.reduce(({ topic, topics }, token) => {
	    const index = UPDATE_EXPRESSION_TOPICS.indexOf(token.toUpperCase());

	    if (index >= 0) {
		return {
		    topic: token,
		    topics
		};
	    } else {
		const str = topics[topic];
		const prefix = str ? str + ' ' : '';
		
		return {
		    topic,
		    topics: {
			...topics,
			[topic]: prefix + token
		    }
		};
	    }
	}, { topic: '', topics: {} })
	.topics;
}

export function stringifyUpdateExpression(parsedUpdateExpression) {
    return Object.keys(parsedUpdateExpression)
	.filter(key => (parsedUpdateExpression[key] || '').trim())
	.reduce((updateExpression, topic) => {
	    return updateExpression + topic + ' ' + parsedUpdateExpression[topic];
	}, '');
}

export function composeUpdateExpressions(...updateExpressions) {
    const composedParsedUpdateExpressions = updateExpressions
	  .map(updateExpression => parseUpdateExpression(updateExpression))
	  .reduce((acc, parsedUpdateExpression) => {
	      const composedTopics = Object.keys(parsedUpdateExpression)
		    .reduce((_acc, topic) => {
			const expression = parsedUpdateExpression[topic] + (acc[topic] ? ', ' + acc[topic] : '');

			return { ..._acc, [topic]: expression };
		    }, {});

	      return { ...acc, ...composedTopics };
	  }, {});

    return stringifyUpdateExpression(composedParsedUpdateExpressions);
}

export function defineExpressionAttributes(attributeValues) {
    return Object.keys(attributeValues)
	.reduce(({ ExpressionAttributeNames, ExpressionAttributeValues }, key) => {
	    const nameKey = '#' + key;
	    const valueKey = ':' + key;
	    const value = attributeValues[key];
	    
	    return {
		ExpressionAttributeNames: {
		    ...ExpressionAttributeNames,
		    [nameKey]: key
		},
		ExpressionAttributeValues: {
		    ...ExpressionAttributeValues,
		    [valueKey]: value
		}
	    };
	}, { ExpressionAttributeNames: {}, ExpressionAttributeValues: {} });
}

export const TX_OP = {
    DELETE: 'delete',
    UPDATE: 'update',
    PUT: 'put',
    GET: 'get'
};

export const TX_ERROR = {
    TX_LOCK_CONTENTION_ERROR: 'TX_LOCK_CONTENTION_ERROR',
    TX_UNKNOWN_ERROR: 'TX_UNKNOWN_ERROR',
    TX_UNSUPPORTED_OPERATION_ERROR: 'TX_UNSUPPORTED_OPERATION_ERROR'
};


export const TX_IMAGE_TABLE_NAME = 'TransactionImages';
