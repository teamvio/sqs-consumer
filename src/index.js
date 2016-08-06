import events from 'events';
import AWS from 'aws-sdk';
import Debug from 'debug';
import Promise from 'bluebird';
const debug = Debug('sqs-consumer');

export class SQSError extends Error {
    name = 'SQSError';
    status = 500;
    constructor(message = '') {
        super(message);
        this.message = message;
    }
}

export class ProcessingError extends Error {
    name = 'ProcessingError';
    status = 500;
    constructor(message = 'Processing error.') {
        super(message);
        this.message = message;
    }
}

const requiredOptions = [
    'queueUrl',
    'handleMessage'
];

function validate(options) {
    requiredOptions.forEach(option => {
        if (!options[option]) throw new Error(`Missing SQS consumer option [${option}].`);
    });

    if (options.batchSize > 10 || options.batchSize < 1) {
        throw new Error('SQS batchSize option must be between 1 and 10.');
    }
}

/**
* An SQS consumer.
* @param {object} options
* @param {string} options.queueUrl
* @param {string} options.region
* @param {function} options.handleMessage
* @param {array} options.attributeNames
* @param {array} options.messageAttributeNames
* @param {number} options.batchSize
* @param {object} options.sqs
* @param {number} options.visibilityTimeout
* @param {number} options.waitTimeSeconds
*/
export default class Consumer extends events.EventEmitter {
    constructor(options) {
        super();
        validate(options);
        this.queueUrl = options.queueUrl;
        this.handleMessage = options.handleMessage;
        this.attributeNames = options.attributeNames || [];
        this.messageAttributeNames = options.messageAttributeNames || [];
        this.stopped = true;
        this.batchSize = options.batchSize || 1;
        this.visibilityTimeout = options.visibilityTimeout || 500;
        this.waitTimeSeconds = options.waitTimeSeconds || 20;
        this.sqs = options.sqs ||
            Promise.promisifyAll(new AWS.SQS({ region: options.region || 'eu-west-1' }));
    }

    /**
    * Start polling for messages.
    */
    start() {
        if (this.stopped) {
            debug('Starting consumer');
            this.stopped = false;
            this.poll();
        }
    }

    /**
    * Stop polling for messages.
    */
    stop() {
        debug('Stopping consumer');
        this.stopped = true;
    }

    poll() {
        const receiveParams = {
            QueueUrl: this.queueUrl,
            AttributeNames: this.attributeNames,
            MessageAttributeNames: this.messageAttributeNames,
            MaxNumberOfMessages: this.batchSize,
            WaitTimeSeconds: this.waitTimeSeconds,
            VisibilityTimeout: this.visibilityTimeout
        };

        if (!this.stopped) {
            debug('Polling for messages');
            this.sqs.receiveMessageAsync(receiveParams)
                .then(response => {
                    this.handleSQSResponse(response);
                })
                .catch(err => {
                    this.emit('error', new SQSError(`SQS receive message failed: ${err.message}`));
                });
        } else {
            this.emit('stopped');
        }
    }

    handleSQSResponse(response) {
        debug('Received SQS response');
        debug(response);
        if (response && response.Messages && response.Messages.length > 0) {
            Promise.map(response.Messages, message => this.processMessage(message))
                .then(() => {
                    this.poll();
                });
        } else {
            // there were no messages, so start polling again
            this.poll();
        }
    }

    processMessage(message) {
        this.emit('message_received', message);
        return this.handleMessage(message)
            .then(() => this.deleteMessage(message))
            .then(() => {
                this.emit('message_processed', message);
                return Promise.resolve();
            })
            .catch(err => {
                if (err.name === SQSError.name) this.emit('error', err);
                else this.emit('processing_error', err);
                return Promise.resolve();
            });
    }

    deleteMessage(message) {
        const deleteParams = {
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle
        };

        debug('Deleting message %s', message.MessageId);
        return this.sqs.deleteMessageAsync(deleteParams)
            .catch(err => {
                throw new SQSError(`SQS delete message failed: ${err.message}`);
            });
    }
}
