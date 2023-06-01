import { AWSError, DynamoDB as DB } from 'aws-sdk';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { PromiseResult } from 'aws-sdk/lib/request';
import { encodeBase32 } from 'geohashing';
import { generate } from 'randomstring';
import { TableType, Table, TableInfo } from '../models/table-info';
import { Message } from '../models/message';

export class DynamoDBDelegate {
    constructor() { }

    public async storeDataToDatabase(message: Message): Promise<void> {
        console.log("DynamoDBDelegate: ", "storeDataToDatabase called");

        const primaryTableName: string = "iot-core-to-dynamo-db-function-2-NimTrack2-80E2PWC3CVKJ";

        const db: DB = new DB();

        await this.lookUpPrimaryTable({ primaryTableName, db, message });

        return;
    }

    private async lookUpPrimaryTable({ primaryTableName, db, message }: { primaryTableName: string; db: DB; message: Message }): Promise<void> {
        console.log("DynamoDBDelegate: ", "lookUpPrimaryTable called");

        const getItem: PromiseResult<DB.GetItemOutput, AWSError> = await db.getItem({
            TableName: primaryTableName,
            Key: {
                "id": {
                    N: `${message.nodeId}`
                }
            }
        }).promise();

        if (getItem.$response.data && getItem.Item) {
            console.info(
                "DynamoDBDelegate: ",
                "lookUpPrimaryTable: ",
                "getItem.$response.data && getItem.Item: ",
                "if block: ",
                `item is ${JSON.stringify(getItem.Item)}`,
                `, data is ${JSON.stringify(getItem.$response.data)}`
            );

            await this.putItem({ tableInfo: new TableInfo({ type: "secondary", name: Converter.unmarshall(getItem.Item).table }), db, message });
        } else {
            console.info(
                "DynamoDBDelegate: ",
                "lookUpPrimaryTable: ",
                "getItem.$response.data && getItem.Item: ",
                "else block: ",
                `item is ${JSON.stringify(getItem.Item)}`,
                `, data is ${JSON.stringify(getItem.$response.data)}`,
            );

            const secondaryTableName: string | null = await this.createSecondaryTable({ id: message.nodeId, db });

            if (secondaryTableName) {
                console.info(
                    "DynamoDBDelegate: ",
                    "lookUpPrimaryTable: ",
                    "getItem.$response.data && getItem.Item: ",
                    "else block: ",
                    "secondaryTableName: ",
                    "if block: ",
                    `item is still ${JSON.stringify(getItem.Item)}`,
                    `, data is still ${JSON.stringify(getItem.$response.data)}`,
                    `, secondary table name is ${secondaryTableName}`
                );

                await this.putItem({ tableInfo: new TableInfo({ type: "primary", name: primaryTableName }), db, message, additionalTableInfo: new TableInfo({ type: "secondary", name: secondaryTableName }) });

                await this.putItem({ tableInfo: new TableInfo({ type: "secondary", name: secondaryTableName }), db, message });
            } else {
                console.info(
                    "DynamoDBDelegate: ",
                    "lookUpPrimaryTable: ",
                    "getItem.$response.data && getItem.Item: ",
                    "else block: ",
                    "secondaryTableName: ",
                    "else block: ",
                    `item is still ${JSON.stringify(getItem.Item)}`,
                    `, data is still ${JSON.stringify(getItem.$response.data)}`,
                    `, but unfortunately could not create secondary table as secondary table name is ${secondaryTableName}`
                );
            }
        }

        return;
    }

    private async createSecondaryTable({ id, db }: { id: number; db: DB }): Promise<string | null> {
        console.log("DynamoDBDelegate: ", "createSecondaryTable called");

        const randomString: string = generate({ length: 12, readable: true, charset: "alphanumeric", capitalization: "uppercase" });

        const createTable: PromiseResult<DB.CreateTableOutput, AWSError> = await db.createTable({
            TableName: `iot-core-to-dynamo-db-function-2-NimTrack2-secondary-table-${id}-${randomString}`,
            AttributeDefinitions: [
                {
                    AttributeName: "timestamp",
                    AttributeType: "N"
                }
            ], KeySchema: [
                {
                    AttributeName: "timestamp",
                    KeyType: "HASH"
                }
            ],
            BillingMode: "PAY_PER_REQUEST"
        }).promise();

        let tableName: string | null = null;

        if (createTable.TableDescription && createTable.TableDescription?.TableName) {
            tableName = createTable.TableDescription?.TableName;

            const waitForTable: PromiseResult<DB.DescribeTableOutput, AWSError> = await db.waitFor("tableExists", { TableName: tableName }).promise();

            console.info(
                "DynamoDBDelegate: ",
                "createSecondaryTable: ",
                "createTable.TableDescription && createTable.TableDescription?.TableName: ",
                "if block: ",
                `table name is ${tableName}`,
                `, table status is ${waitForTable.Table?.TableStatus}`
            );
        } else {
            console.info(
                "DynamoDBDelegate: ",
                "createSecondaryTable: ",
                "createTable.TableDescription && createTable.TableDescription?.TableName: ",
                "else block: ",
                `table name is ${tableName}`
            );
        }

        return tableName;
    }

    private putItem({ tableInfo, db, message, additionalTableInfo }: { tableInfo: TableInfo; db: DB; message: Message; additionalTableInfo?: TableInfo | null }): Promise<PromiseResult<DB.PutItemOutput, AWSError>> {
        console.log("DynamoDBDelegate: ", "putItem called");

        let item: DB.PutItemInputAttributeMap;

        switch (tableInfo.table.type) {
            case "primary":
                item = {
                    "id": {
                        N: `${message.nodeId}`
                    }
                };

                if (additionalTableInfo && additionalTableInfo.table.type === "secondary") {
                    item.table = {
                        S: additionalTableInfo.table.name
                    };
                }

                break;
            case "secondary":
                item = this.computeItem({ message });
                break;
        }

        return db.putItem({
            TableName: tableInfo.table.name,
            Item: item
        }).promise();
    }

    private computeItem({ message }: { message: Message }): {
        timestamp: {
            N: string;
        };
        coordinates: {
            M: {
                hash: {
                    S: string;
                };
                lat_lng: {
                    L: {
                        N: string;
                    }[];
                };
            };
        };
        battery_level: {
            N: string;
        };
    } {
        console.log("DynamoDBDelegate: ", "computeDataItem called");

        return {
            "timestamp": {
                N: `${this.computeUnixTimestamp()}`
            },
            "coordinates": {
                M: {
                    "hash": {
                        S: `${this.computeGeohash({
                            latitude: message.latitude,
                            longitude: message.longitude
                        })}`
                    },
                    "lat_lng": {
                        L: [
                            {
                                N: `${message.latitude}`,
                            },
                            {
                                N: `${message.longitude}`
                            }
                        ]
                    }
                }
            },
            "battery_level": {
                N: `${message.batteryLevel}`
            }
        };
    }

    private computeGeohash({ latitude, longitude }: { latitude: number, longitude: number }): string {
        console.log("DynamoDBDelegate: ", "computeGeohash called");

        const hash: string = encodeBase32(latitude, longitude, 9);
        console.info(
            "DynamoDBDelegate: ",
            "computeGeohash: ",
            `provided latitude is ${latitude}`,
            `, provided longitude is ${longitude}`,
            `, computed geohash is ${hash}`
        );

        return hash;
    }

    private computeUnixTimestamp(): number {
        console.log("DynamoDBDelegate: ", "computeUnixTimestamp called");

        const date: Date = new Date();
        const unixTimestamp: number = Math.floor(date.getTime() / 1000);

        console.info(
            "DynamoDBDelegate: ",
            "computeUnixTimestamp: ",
            `date is ${date}`,
            `, unix timestamp is ${unixTimestamp}`
        );

        return unixTimestamp;
    }
}