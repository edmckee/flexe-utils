const fetch = require('node-fetch');

//Setup AWS services
const AWS = require("aws-sdk");


let SETTINGS = {};

let STATUS = {
    NEW: "NEW",
    NEW_ERR: "NEW_ERR",
    QUEUE: "QUEUE",
    QUEUE_ERR: "QUEUE_ERR",
    FLEXE_QUEUE: "FLEXE_QUEUE",
    FLEXE_QUEUE_ERR: "FLEXE_QUEUE_ERR",
    PO:"PO",
    PO_ERR: "PO_ERR",
    COMPLETE: "COMPLETE",
    COMPLETE_ERR: "COMPLETE_ERR",
    INVENTORY_ERR: "INVENTORY_ERR",
    PENDING: "PENDING",
    PARTIAL: "PARTIAL",
    CANCELED: "CANCELED",
    CANCELED_ERR: "CANCELED_ERR",
    PAYMENT_PENDING: "PAYMENT_PENDING",
    ADDRESS_ERR: "ADDRESS_ERR"
};

let FLEXE_STATUS = {
    NEW: "NEW",
    NEW_ERR: "NEW_ERR",
    QUEUE: "QUEUE",
    QUEUE_ERR: "QUEUE_ERR",
    FLEXE: "FLEXE",
    FLEXE_ERR: "FLEXE_ERR",
    PO:"PO",
    PO_ERR: "PO_ERR",
    INVENTORY_ERR: "INVENTORY_ERR",
    BACKORDER: "BACKORDER",
    BACKORDER_QUEUE: "BACKORDER_QUEUE",
    BACKORDER_ERR: "BACKORDER_ERR",
    COMPLETE: "COMPLETE",
    COMPLETE_ERR: "COMPLETE_ERR",
    CANCELED: "CANCELED",
    CANCELED_ERR: "CANCELED_ERR"
};


module.exports = {
    init: async(customerId) => {
        for (let k in process.env) {
            SETTINGS[k] = process.env[k];
        }

        return new Promise(async(resolve, reject) => {
            let params = {
                TableName: "ServiceConfig"
            };

            try
            {
                //get global config from DynamoDB
                let result = await module.exports.DynamoDBScan(params);
                result.Items.forEach(a => SETTINGS[a.name] = a.value);

                //get customer specific config from mysql
                let knex = module.exports.GetKnexDBConnection();
                let rows = await knex.select('Items')
                    .from('ServiceConfig')
                    .where({customerId: customerId});

                console.log("rows = " + JSON.stringify(rows));

                rows.Items.forEach(a => SETTINGS[a.name] = a.value);

                console.log("SETTINGS:" + JSON.stringify(SETTINGS));
                return resolve(SETTINGS);
            }
            catch(error) {
                console.log("Error Init(): " + error);
                return reject(error);
            }
        });
    },
    Settings: SETTINGS,
    Status: STATUS,
    FlexeStatus: FLEXE_STATUS,
    getSetting: (key, defaultValue) => {
        return SETTINGS[key] ? SETTINGS[key] : defaultValue;
    },
    getAPIKeyHeader: () => {
        let header = {
            "x-api-key": module.exports.getSetting("AWS_API_KEY", ""),
            "Content-Type": "application/json"
        };
        
        return header;
    },
    RESP_STR: (err, res) => (null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : res,
        headers: {
            'Content-Type': 'application/json',
        }
    }),
    CORS_RESP_STR: (err, res) => (null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : res,
         headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials' : true
        }
    }),
    putShopifyOrderStatus: (id, status) => {
        return new Promise(async(resolve, reject) => {
            try {
                let url = module.exports.getSetting("ShopifyOrdersAPIURL") + "/" + id;
                let body = {
                    "order_status": status
                };
                let header = module.exports.getAPIKeyHeader();

                await module.exports.UrlPut(url, body, header);
                resolve(true);
            }
            catch (err) {
                console.error(err);
                resolve(true);
            }
        });
    },
    putFlexeOrderStatus: (id, status) => {
        return new Promise(async(resolve, reject) => {
            try {
                let url = module.exports.getSetting("FlexeOrdersAPIURL") + "/" + id;
                let body = {
                    "order_status": status
                };
                let header = module.exports.getAPIKeyHeader();

                await module.exports.UrlPut(url, body, header);
                resolve(true);
            }
            catch (err) {
                console.error(err);
                resolve(true);
            }
        });
    },
    UrlGet: (url, headers) => {
        return new Promise((resolve, reject) => {
            console.log("UrlGet:" + url);
            fetch(url, {
                method: 'get',
                headers: headers
            }).then(res => res.json()).then(json => {
                resolve(json);
            }).catch((error) => {
                reject(error);
            });
        });
    },
    UrlPost: (url, body, headers) => {
        return new Promise((resolve, reject) => {
            console.log("UrlPost:" + url);
            fetch(url, {
                method: 'post',
                body: JSON.stringify(body),
                headers: headers,
            }).then(res => res.json()).then(json => {
                resolve(json);
            }).catch((error) => {
                reject(error);
            });
        });
    },
    UrlPut: (url, body, headers) => {
        return new Promise((resolve, reject) => {
            console.log("UrlPut:" + url);
            fetch(url, {
                method: 'put',
                body: JSON.stringify(body),
                headers: headers,
            }).then(res => res.json()).then(json => {
                resolve(json);
            }).catch((error) => {
                reject(error);
            });
        });
    },
    SnsSend: (topicArn, subject, message) => {
        return new Promise((resolve, reject) => {

            let snsParams = {
                Message: JSON.stringify(message),
                Subject: subject,
                TopicArn: topicArn
            };

            console.info("SnsSend:params:", snsParams);

            let sns = new AWS.SNS();
            sns.publish(snsParams, function(err, result) {
                if (err) {
                    console.error("SnsSend:", err, err.stack);
                    reject(err);

                }
                else {
                    console.log("SnsSend:result: " + JSON.stringify(result));
                    resolve(true);
                }
            });
        });
    },
    DynamoDBGet: (params) => {
        return new Promise((resolve, reject) => {

            let dynamo = new AWS.DynamoDB.DocumentClient();

            console.info("DynamoDBGet:params:", params);

            dynamo.get(params, function(err, result) {
                if (err) {
                    console.error("DynamoDBGet:", err, err.stack);
                    reject(err);
                }
                else {
                    console.log("DynamoDBGet:result: " + JSON.stringify(result));
                    resolve(result);
                }
            });
        });
    },
    DynamoDBScan: (params) => {
        return new Promise((resolve, reject) => {

            let dynamo = new AWS.DynamoDB.DocumentClient();

            console.info("DynamoDBScan:params:", params);

            let data = { Items: [], Count: 0, ScannedCount: 0 };

            let scan_callback = async function(err, result) {
                if (err) {
                    console.error("DynamoDBScan:", err, err.stack);
                    reject(err);
                }
                else {
                    console.log("DynamoDBScan:result: " + JSON.stringify(result));

                    data.Items = data.Items.concat(result.Items);
                    data.Count = data.Count + result.Count;
                    data.ScannedCount = data.ScannedCount + result.ScannedCount;

                    // continue scanning if we have more records, because
                    // scan can retrieve a maximum of 1MB of data
                    if (typeof result.LastEvaluatedKey != "undefined") {
                        console.log("Scanning for more...");
                        params.ExclusiveStartKey = result.LastEvaluatedKey;
                        await dynamo.scan(params, scan_callback);
                    }
                    else {
                        resolve(data);
                    }
                }
            };

            dynamo.scan(params, scan_callback);
        });
    },
    DynamoDBPut: (params) => {
        return new Promise((resolve, reject) => {

            var dynamo = new AWS.DynamoDB.DocumentClient({ "convertEmptyValues": true });

            console.info("DynamoDBPut:params:", params);

            dynamo.put(params, function(err, result) {
                if (err) {
                    console.error("DynamoDBPut:", err, err.stack);
                    reject(err);
                }
                else {
                    console.log("DynamoDBPut:result: " + JSON.stringify(result));
                    resolve("SUCCESS");
                }
            });
        });
    },
    DynamoDBUpdate: (params) => {
        return new Promise((resolve, reject) => {

            var dynamo = new AWS.DynamoDB.DocumentClient({ "convertEmptyValues": true });

            console.info("DynamoDBUpdate:params:", params);

            dynamo.update(params, function(err, result) {
                if (err) {
                    console.error("DynamoDBUpdate:", err, err.stack);
                    reject(err);
                }
                else {
                    console.log("DynamoDBUpdate:result: " + JSON.stringify(result));
                    resolve("SUCCESS");
                }
            });
        });
    },
    DynamoDBDelete: (params) => {
        return new Promise((resolve, reject) => {

            var dynamo = new AWS.DynamoDB.DocumentClient();

            console.info("DynamoDBDelete:params:", params);

            dynamo.delete(params, function(err, result) {
                if (err) {
                    console.error("DynamoDBDelete:", err, err.stack);
                    reject(err);
                }
                else {
                    console.log("DynamoDBDelete:result: " + JSON.stringify(result));
                    resolve("SUCCESS");
                }
            });
        });
    },
    DynamoDBTransactWrite: (params) => {
        return new Promise((resolve, reject) => {

            var dynamo = new AWS.DynamoDB.DocumentClient({ "convertEmptyValues": true });

            console.info("DynamoDBTransactWrite:params:", params);

            dynamo.transactWrite(params, function(err, result) {
                if (err) {
                    //check for transaction conflict
                    if (err.message.includes("TransactionConflict")) {
                        console.error("DynamoDBTransactWrite:TransactionConflict trying again...");
                        //try again
                        dynamo.transactWrite(params, function(err, result) {
                            if (err) {
                                if (err.message.includes("TransactionConflict")) {
                                    console.error("DynamoDBTransactWrite:TransactionConflict ", err, err.stack);
                                    reject(err);
                                }
                                else {
                                    console.error("DynamoDBTransactWrite:", err, err.stack);
                                    reject(err);
                                }
                            }
                            else {
                                console.log("DynamoDBTransactWrite:result: " + JSON.stringify(result));
                                resolve("SUCCESS");
                            }
                        });
                    }
                    else {
                        console.error("DynamoDBTransactWrite:", err, err.stack);
                        reject(err);
                    }
                }
                else {
                    console.log("DynamoDBTransactWrite:result: " + JSON.stringify(result));
                    resolve("SUCCESS");
                }
            });
        });
    },
    DynamoDBQuery: (params) => {
        return new Promise((resolve, reject) => {

            let dynamo = new AWS.DynamoDB.DocumentClient();

            console.info("DynamoDBQuery:params:", params);

            let data = { Items: [], Count: 0, ScannedCount: 0 };

            let query_callback = async function(err, result) {
                if (err) {
                    console.error("DynamoDBQuery:", err, err.stack);
                    reject(err);
                }
                else {
                    console.log("DynamoDBQuery:result: " + JSON.stringify(result));

                    data.Items = data.Items.concat(result.Items);
                    data.Count = data.Count + result.Count;
                    data.ScannedCount = data.ScannedCount + result.ScannedCount;

                    // continue scanning if we have more records, because
                    // scan can retrieve a maximum of 1MB of data
                    if (typeof result.LastEvaluatedKey != "undefined") {
                        console.log("Scanning for more...");
                        params.ExclusiveStartKey = result.LastEvaluatedKey;
                        await dynamo.query(params, query_callback);
                    }
                    else {
                        resolve(data);
                    }
                }
            };

            dynamo.query(params, query_callback);
        });
    },
    Alert: (orderid, message, exception, share) => {
        return new Promise(async (resolve, reject)=>{
            let data = {
                orderid, message
            };
            if(share) data.share=true;
            if(exception) data.exception = exception;
            try {
                console.log('Alert',orderid, message, module.exports.getSetting('AlertAPIURL'));
                await module.exports.UrlPost(module.exports.getSetting('AlertAPIURL'),data,module.exports.getAPIKeyHeader());
                resolve(true);
            } catch(err) {
                console.error(err);
                resolve(true);
            }
        });
    },
    GetKnexDBConnection: () => {
        const knex = require('knex')({
            client: 'mysql2',
            connection: {
                host : module.exports.getSetting("mysql_connect_url"),
                user : module.exports.getSetting("mysql_user"),
                password : module.exports.getSetting("mysql_pwd"),
                database : module.exports.getSetting("mysql_db")
            }
        });

        return knex;
    }
    
};
