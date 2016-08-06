
const requiredOptions = [
    'queueUrl',
    'handleMessage'
];

export default function validate(options) {
    requiredOptions.forEach(option => {
        if (!options[option]) throw new Error(`Missing SQS consumer option [${option}].`);
    });

    if (options.batchSize > 10 || options.batchSize < 1) {
        throw new Error('SQS batchSize option must be between 1 and 10.');
    }
}
