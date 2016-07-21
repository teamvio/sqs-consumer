/* eslint-disable import/no-extraneous-dependencies */
import assert from 'assert';
import sinon from 'sinon';
import Promise from 'bluebird';
import Consumer from '../src';
require('sinon-as-promised')(Promise);


describe('Consumer', () => {
    let consumer;
    let handleMessage;
    let sqs;
    const response = {
        Messages: [{
            ReceiptHandle: 'receipt-handle',
            MessageId: '123',
            Body: 'body'
        }]
    };

    beforeEach(() => {
        handleMessage = sinon.stub().resolves();
        sqs = sinon.mock();
        sqs.receiveMessageAsync = sinon.stub().resolves(response);
        sqs.receiveMessageAsync.onSecondCall().resolves();
        sqs.deleteMessageAsync = sinon.stub().resolves();
        consumer = new Consumer({
            queueUrl: 'some-queue-url',
            region: 'some-region',
            authenticationErrorTimeout: 20,
            handleMessage,
            sqs
        });
    });

    it('requires a queueUrl to be set', () => {
        assert.throws(() => {
            new Consumer({
                region: 'some-region',
                handleMessage
            });
        });
    });

    it('requires a handleMessage function to be set', () => {
        assert.throws(() => {
            new Consumer({
                region: 'some-region',
                queueUrl: 'some-queue-url'
            });
        });
    });

    it('requires the batchSize option to be no greater than 10', () => {
        assert.throws(() => {
            new Consumer({
                region: 'some-region',
                queueUrl: 'some-queue-url',
                handleMessage,
                batchSize: 11
            });
        });
    });

    it('requires the batchSize option to be greater than 0', () => {
        assert.throws(() => {
            new Consumer({
                region: 'some-region',
                queueUrl: 'some-queue-url',
                handleMessage,
                batchSize: -1
            });
        });
    });

    describe('.start', () => {
        it('fires an error event when an error occurs receiving a message', done => {
            const receiveErr = new Error('Receive error');
            sqs.receiveMessageAsync.rejects(receiveErr);

            consumer.on('error', err => {
                assert.ok(err);
                assert.equal(err.message, 'SQS receive message failed: Receive error');
                done();
            });

            consumer.start();
        });

        it('fires an error event when an error occurs deleting a message', done => {
            const deleteErr = new Error('Delete error');
            sqs.deleteMessageAsync.rejects(deleteErr);

            consumer.on('error', err => {
                assert.ok(err);
                assert.equal(err.message, 'SQS delete message failed: Delete error');
                consumer.stop();
                done();
            });

            consumer.start();
        });

        it('fires an error event when an error occurs processing a message', done => {
            const processingErr = new Error('Processing error');

            handleMessage.rejects(processingErr);

            consumer.on('processing_error', err => {
                assert.equal(err, processingErr);
                consumer.stop();
                done();
            });

            consumer.start();
        });

        it('waits before repolling when a credentials error occurs', done => {
            const credentialsErr = {
                code: 'CredentialsError',
                message: 'Missing credentials in config'
            };

            sqs.receiveMessageAsync.rejects(credentialsErr);

            consumer.on('error', () => {
                setTimeout(() => {
                    sinon.assert.calledOnce(sqs.receiveMessageAsync);
                }, 15);
                setTimeout(() => {
                    sinon.assert.calledTwice(sqs.receiveMessageAsync);
                    done();
                }, 21);
            });

            consumer.start();
        });

        it('waits before repolling when a 403 error occurs', done => {
            const invalidSignatureErr = {
                statusCode: 403,
                message: 'The security token included in the request is invalid'
            };

            sqs.receiveMessageAsync.rejects(invalidSignatureErr);

            consumer.on('error', () => {
                setTimeout(() => {
                    sinon.assert.calledOnce(sqs.receiveMessageAsync);
                }, 1200);
                setTimeout(() => {
                    sinon.assert.calledTwice(sqs.receiveMessageAsync);
                    done();
                }, 2400);
            });

            consumer.start();
        });

        it('fires a message_received event when a message is received', done => {
            consumer.on('message_received', message => {
                assert.equal(message, response.Messages[0]);
                consumer.stop();
                done();
            });

            consumer.start();
        });

        it('fires a message_processed event when a message is successfully deleted', done => {
            consumer.on('message_processed', message => {
                assert.equal(message, response.Messages[0]);
                consumer.stop();
                done();
            });

            consumer.start();
        });

        it('calls the handleMessage function when a message is received', done => {
            consumer.on('message_processed', () => {
                sinon.assert.calledWith(handleMessage, response.Messages[0]);
                consumer.stop();
                done();
            });

            consumer.start();
        });

        it('deletes the message when the handleMessage callback is called', done => {
            handleMessage.reolves();

            consumer.on('message_processed', () => {
                sinon.assert.calledWith(sqs.deleteMessageAsync, {
                    QueueUrl: 'some-queue-url',
                    ReceiptHandle: 'receipt-handle'
                });
                done();
            });

            consumer.start();
        });

        it('doesn\'t delete the message when a processing error is reported', () => {
            handleMessage.rejects(new Error('Processing error'));

            consumer.on('processing_error', () => {
                // ignore the error
            });

            consumer.start();

            sinon.assert.notCalled(sqs.deleteMessageAsync);
        });

        it('consumes another message once one is processed', done => {
            sqs.receiveMessageAsync.onSecondCall().resolves(response);
            sqs.receiveMessageAsync.onThirdCall().resolves();

            consumer.start();
            setTimeout(() => {
                sinon.assert.calledTwice(handleMessage);
                done();
            }, 10);
        });

        it('doesn\'t consume more messages when called multiple times', () => {
            sqs.receiveMessageAsync = sinon.stub().returns();
            consumer.start();
            consumer.start();
            consumer.start();
            consumer.start();
            consumer.start();

            sinon.assert.calledOnce(sqs.receiveMessageAsync);
        });

        it('consumes multiple messages when the batchSize is greater than 1', done => {
            sqs.receiveMessageAsync.resolves({
                Messages: [
                    {
                        ReceiptHandle: 'receipt-handle-1',
                        MessageId: '1',
                        Body: 'body-1'
                    },
                    {
                        ReceiptHandle: 'receipt-handle-2',
                        MessageId: '2',
                        Body: 'body-2'
                    },
                    {
                        ReceiptHandle: 'receipt-handle-3',
                        MessageId: '3',
                        Body: 'body-3'
                    }
                ]
            });

            consumer = new Consumer({
                queueUrl: 'some-queue-url',
                messageAttributeNames: ['attribute-1', 'attribute-2'],
                region: 'some-region',
                handleMessage,
                batchSize: 3,
                sqs
            });

            consumer.start();

            setTimeout(() => {
                sinon.assert.calledWith(sqs.receiveMessageAsync, {
                    QueueUrl: 'some-queue-url',
                    AttributeNames: [],
                    MessageAttributeNames: ['attribute-1', 'attribute-2'],
                    MaxNumberOfMessages: 3,
                    WaitTimeSeconds: 20,
                    VisibilityTimeout: undefined
                });
                sinon.assert.callCount(handleMessage, 3);
                done();
            }, 10);
        });

        it('consumes messages with message attibute \'ApproximateReceiveCount\'', done => {
            const messageWithAttr = {
                ReceiptHandle: 'receipt-handle-1',
                MessageId: '1',
                Body: 'body-1',
                Attributes: {
                    ApproximateReceiveCount: 1
                }
            };

            sqs.receiveMessageAsync.resolves({
                Messages: [messageWithAttr]
            });

            consumer = new Consumer({
                queueUrl: 'some-queue-url',
                attributeNames: ['ApproximateReceiveCount'],
                region: 'some-region',
                handleMessage,
                sqs
            });

            consumer.on('message_received', message => {
                sinon.assert.calledWith(sqs.receiveMessageAsync, {
                    QueueUrl: 'some-queue-url',
                    AttributeNames: ['ApproximateReceiveCount'],
                    MessageAttributeNames: [],
                    MaxNumberOfMessages: 1,
                    WaitTimeSeconds: 20,
                    VisibilityTimeout: undefined
                });
                assert.equal(message, messageWithAttr);
                done();
            });

            consumer.start();
        });
    });

    describe('.stop', () => {
        beforeEach(() => {
            sqs.receiveMessageAsync.onSecondCall().resolves(response);
            sqs.receiveMessageAsync.onThirdCall().resolves();
        });

        it('stops the consumer polling for messages', done => {
            setTimeout(() => {
                sinon.assert.calledOnce(handleMessage);
                done();
            }, 50);

            consumer.start();
            consumer.stop();
        });

        it('fires a stopped event when last poll occurs after stopping', done => {
            const handleStop = sinon.stub().returns();

            consumer.on('stopped', handleStop);

            setTimeout(() => {
                sinon.assert.calledOnce(handleStop);
                done();
            }, 50);

            consumer.start();
            consumer.stop();
        });

        it('fires a stopped event only once when stopped multiple times', done => {
            const handleStop = sinon.stub().returns();

            consumer.on('stopped', handleStop);

            setTimeout(() => {
                sinon.assert.calledOnce(handleStop);
                done();
            }, 50);

            consumer.start();
            consumer.stop();
            consumer.stop();
            consumer.stop();
        });

        it('fires a stopped event a second time if started and stopped twice', done => {
            const handleStop = sinon.stub().returns();

            consumer.on('stopped', handleStop);

            setTimeout(() => {
                sinon.assert.calledTwice(handleStop);
                done();
            }, 50);

            consumer.start();
            consumer.stop();
            consumer.start();
            consumer.stop();
        });
    });
});
