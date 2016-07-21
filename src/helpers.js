
const requiredOptions = [
    'queueUrl',
    'handleMessage'
];

export function validate(options) {
    requiredOptions.forEach(option => {
        if (!options[option]) throw new Error(`Missing SQS consumer option [${option}].`);
    });

    if (options.batchSize > 10 || options.batchSize < 1) {
        throw new Error('SQS batchSize option must be between 1 and 10.');
    }
}

export function isAuthenticationError(err) {
    return (err.statusCode === 403 || err.code === 'CredentialsError');
}
