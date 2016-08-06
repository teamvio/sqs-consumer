'use strict';

Object.defineProperty(exports, "__esModule", {
    value: true
});
exports.ProcessingError = exports.SQSError = undefined;

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

var _events = require('events');

var _events2 = _interopRequireDefault(_events);

var _awsSdk = require('aws-sdk');

var _awsSdk2 = _interopRequireDefault(_awsSdk);

var _debug = require('debug');

var _debug2 = _interopRequireDefault(_debug);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var debug = (0, _debug2.default)('sqs-consumer');

var SQSError = exports.SQSError = (function (_Error) {
    _inherits(SQSError, _Error);

    function SQSError() {
        var message = arguments.length <= 0 || arguments[0] === undefined ? '' : arguments[0];

        _classCallCheck(this, SQSError);

        var _this = _possibleConstructorReturn(this, Object.getPrototypeOf(SQSError).call(this, message));

        _this.name = 'SQSError';
        _this.status = 500;

        _this.message = message;
        return _this;
    }

    return SQSError;
})(Error);

var ProcessingError = exports.ProcessingError = (function (_Error2) {
    _inherits(ProcessingError, _Error2);

    function ProcessingError() {
        var message = arguments.length <= 0 || arguments[0] === undefined ? 'Processing error.' : arguments[0];

        _classCallCheck(this, ProcessingError);

        var _this2 = _possibleConstructorReturn(this, Object.getPrototypeOf(ProcessingError).call(this, message));

        _this2.name = 'ProcessingError';
        _this2.status = 500;

        _this2.message = message;
        return _this2;
    }

    return ProcessingError;
})(Error);

var requiredOptions = ['queueUrl', 'handleMessage'];

function validate(options) {
    requiredOptions.forEach(function (option) {
        if (!options[option]) throw new Error('Missing SQS consumer option [' + option + '].');
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

var Consumer = (function (_events$EventEmitter) {
    _inherits(Consumer, _events$EventEmitter);

    function Consumer(options) {
        _classCallCheck(this, Consumer);

        var _this3 = _possibleConstructorReturn(this, Object.getPrototypeOf(Consumer).call(this));

        validate(options);
        _this3.queueUrl = options.queueUrl;
        _this3.handleMessage = options.handleMessage;
        _this3.attributeNames = options.attributeNames || [];
        _this3.messageAttributeNames = options.messageAttributeNames || [];
        _this3.stopped = true;
        _this3.batchSize = options.batchSize || 1;
        _this3.visibilityTimeout = options.visibilityTimeout || 500;
        _this3.waitTimeSeconds = options.waitTimeSeconds || 20;
        _this3.sqs = options.sqs || _bluebird2.default.promisifyAll(new _awsSdk2.default.SQS({ region: options.region || 'eu-west-1' }));
        return _this3;
    }

    /**
    * Start polling for messages.
    */

    _createClass(Consumer, [{
        key: 'start',
        value: function start() {
            if (this.stopped) {
                debug('Starting consumer');
                this.stopped = false;
                this.poll();
            }
        }

        /**
        * Stop polling for messages.
        */

    }, {
        key: 'stop',
        value: function stop() {
            debug('Stopping consumer');
            this.stopped = true;
        }
    }, {
        key: 'poll',
        value: function poll() {
            var _this4 = this;

            var receiveParams = {
                QueueUrl: this.queueUrl,
                AttributeNames: this.attributeNames,
                MessageAttributeNames: this.messageAttributeNames,
                MaxNumberOfMessages: this.batchSize,
                WaitTimeSeconds: this.waitTimeSeconds,
                VisibilityTimeout: this.visibilityTimeout
            };

            if (!this.stopped) {
                debug('Polling for messages');
                this.sqs.receiveMessageAsync(receiveParams).then(function (response) {
                    _this4.handleSQSResponse(response);
                }).catch(function (err) {
                    _this4.emit('error', new SQSError('SQS receive message failed: ' + err.message));
                });
            } else {
                this.emit('stopped');
            }
        }
    }, {
        key: 'handleSQSResponse',
        value: function handleSQSResponse(response) {
            var _this5 = this;

            debug('Received SQS response');
            debug(response);
            if (response && response.Messages && response.Messages.length > 0) {
                _bluebird2.default.map(response.Messages, function (message) {
                    return _this5.processMessage(message);
                }).then(function () {
                    _this5.poll();
                });
            } else {
                // there were no messages, so start polling again
                this.poll();
            }
        }
    }, {
        key: 'processMessage',
        value: function processMessage(message) {
            var _this6 = this;

            this.emit('message_received', message);
            return this.handleMessage(message).then(function () {
                return _this6.deleteMessage(message);
            }).then(function () {
                _this6.emit('message_processed', message);
                return _bluebird2.default.resolve();
            }).catch(function (err) {
                if (err.name === SQSError.name) _this6.emit('error', err);else _this6.emit('processing_error', err);
                return _bluebird2.default.resolve();
            });
        }
    }, {
        key: 'deleteMessage',
        value: function deleteMessage(message) {
            var deleteParams = {
                QueueUrl: this.queueUrl,
                ReceiptHandle: message.ReceiptHandle
            };

            debug('Deleting message %s', message.MessageId);
            return this.sqs.deleteMessageAsync(deleteParams).catch(function (err) {
                throw new SQSError('SQS delete message failed: ' + err.message);
            });
        }
    }]);

    return Consumer;
})(_events2.default.EventEmitter);

exports.default = Consumer;