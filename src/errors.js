
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
