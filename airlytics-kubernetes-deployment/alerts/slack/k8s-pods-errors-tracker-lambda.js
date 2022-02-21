var aws = require('aws-sdk'),
    https = require('https'),
    zlib = require('zlib');
util = require('util');

// If you've used KMS to encrypt your slack, insert your CiphertextBlob here
var ENCRYPTED_URL = 'AQEC1423...';

// IF NOT, you can take the risk to insert your Slack URL here
// e.g. '/services/QWERTY/ASDFGHJ/zxYTinNLK';
var UNENCRYPTED_URL = "UNENCRYPTED_URL"

// Your Slack channel name goes here
var CHANNEL = "CHANNEL";
var component_owner = "<SLACK_ID>"
var default_user_name = "default_user_name";
var component_name = "component_name";


// These words in a log entry will trigger a red color in Slack
var DANGER_MESSAGES = ["ERROR", "Exception"];

// These words in a log entry will trigger a yellow color in Slack
var WARNING_MESSAGES = ["WARNING"];

/* OK, you can stop touching things now */

var config = {
    hostName: 'hooks.slack.com',
    maxAttempts: 10
};

var cloudWatchLogs = new aws.CloudWatchLogs({
    apiVersion: '2014-03-28'
});

var encryptedSlackUrl = {
    CiphertextBlob: new Buffer(ENCRYPTED_URL, 'base64')
};

var kms = new aws.KMS({
    apiVersion: '2014-11-01'
});

if (!UNENCRYPTED_URL) {
    kms.decrypt(encryptedSlackUrl, function (error, data) {
        if (error) {
            config.tokenInitError = error;
            console.log(error);
        } else {
            config.url = data.Plaintext.toString('ascii');
        }
    });
} else {
    config.url = UNENCRYPTED_URL;
}

exports.handler = function (event, context) {
    var payload = new Buffer(event.awslogs.data, 'base64');


    zlib.gunzip(payload, function (error, result) {
        if (error) {
            context.fail(error);
        } else {
            var result_parsed = JSON.parse(result.toString('ascii'));
            var parsedEvents = result_parsed.logEvents.map(function (logEvent) {
                return parseEvent(logEvent, result_parsed.logGroup, result_parsed.logStream);
            });
            postToSlack(parsedEvents);
        }
    });

    function parseEvent(logEvent, logGroupName, logStreamName) {
        return {
            message: logEvent.message,
            logGroupName: logGroupName,
            logStreamName: logStreamName,
            timestamp: new Date(logEvent.timestamp).toISOString()
        };
    }

    function getErrorContactPerson(pod_name) {
        if (pod_name.indexOf("component_name") != -1) {
            return component_owner;
        }
    }

    function getWebHookByErrorLevel(message) {
        return UNENCRYPTED_URL;
    }

    function getSeverityLevel(message) {
        var severity = "good";

        var dangerMessages = [
            "Error",
            "Exception"
        ];

        var warningMessages = [
            "Warning"
        ];

        for (var dangerMessagesItem in DANGER_MESSAGES) {
            if (message.indexOf(DANGER_MESSAGES[dangerMessagesItem]) != -1) {
                severity = "danger";
                break;
            }
        }

        if (severity == "good") {
            for (var warningMessagesItem in WARNING_MESSAGES) {
                if (message.indexOf(WARNING_MESSAGES[warningMessagesItem]) != -1) {
                    severity = "warning";
                    break;
                }
            }
        }
        return severity;
    }

    function postToSlack(parsedEvents, attempt) {
        if (!config.url) {
            if (!attempt) attempt = 1;
            if (config.tokenInitError) {
                console.log('Error in decrypt the token. Not retrying.');
                return context.fail(config.tokenInitError);
            }
            if (attempt > config.maxAttempts) {
                console.log('Decrypt timed out');
                return context.fail("Timeout");
            }
            console.log('Cannot flush logs since authentication token has not been initialized yet. Retrying in 100ms');
            setTimeout(function () {
                postToSlack(parsedEvents, attempt + 1)
            }, 100);
            return
        }

        var logs = parsedEvents.map(function (e) {

            try {
                return JSON.parse(e.message).log
            } catch (e) {
            }

        }).join('\n');

        var messages = parsedEvents.map(function (e) {
            return e.message
        }).join('\n');
        var logGroup = parsedEvents[0] && parsedEvents[0].logGroupName || 'Missing logGroup';


        var pod_name = 'Missing component';
        var error = null;

        try {
            if (JSON.parse(parsedEvents[0].message)) {
                if (JSON.parse(parsedEvents[0].message).kubernetes) {
                    pod_name = JSON.parse(parsedEvents[0].message).kubernetes.pod_name;
                }
            }
        } catch (e) {
            error = e.message;
        }

        pod_name = pod_name.split("-").slice(0, pod_name.split("-").length - 2).join("-")


        try {
            var options = {
                method: 'POST',
                hostname: config.hostName,
                port: 443,
                path: getWebHookByErrorLevel(logs),
                headers: {
                    'Content-Type': 'application/json'
                }
            };

            var postData = {
                "channel": CHANNEL,
                "username": default_user_name,
                "text": getErrorContactPerson(pod_name) + " *" + pod_name + "*",
                "icon_emoji": ":alert:",
                "as_user": false
            };

            // filter out string "ERROR"
            if ((logs != null && logs.indexOf("\"ERROR\"") != -1) || (logs != null && logs.indexOf("_ERROR") != -1) || (logs != null && logs.indexOf("[warn]") != -1)) {
                return;
            }

            if (logs.indexOf("x-api-key is missing: Request:") != -1) {
                return;
            }

            postData.attachments = [
                {
                    "color": getSeverityLevel(messages),
                    "text": error == null ? logs : (error + " " + parsedEvents[0].message)
                }
            ];


            var req = https.request(options, function (res) {
                res.setEncoding('utf8');
                res.on('data', function (chunk) {
                    context.done(null, "OK");
                });
            });

            req.on('error', function (e) {
                context.fail(e.message);
            });

            req.write(util.format("%j", postData));
            req.end();

        } catch (ex) {
            console.log(ex.message);
            context.fail(ex.message);
        }
    }
};
