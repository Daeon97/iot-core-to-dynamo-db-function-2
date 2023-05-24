import { AWSError, DynamoDB as DB } from 'aws-sdk';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { PromiseResult } from 'aws-sdk/lib/request';
import { encodeBase32 } from 'geohashing';
import { Message } from '../models/message';

export class DynamoDBDelegate {
    constructor() { }

    public async storeDataToDatabase(message: Message): Promise<void> {
        console.log("DynamoDBDelegate: ", "storeDataToDatabase called");

        const tableName: string = "iot-core-to-dynamo-db-function-NimTrack-1880A47TAKPHB";

        const db: DB = new DB();

        await this.putOrUpdateItem({ tableName, db, message });

        return;
    }

    private async putOrUpdateItem({ tableName, db, message }: { tableName: string; db: DB; message: Message }): Promise<void> {
        console.log("DynamoDBDelegate: ", "putOrUpdateItem called");

        const getItem: PromiseResult<DB.GetItemOutput, AWSError> = await db.getItem({
            TableName: tableName,
            Key: {
                "id": {
                    N: `${message.nodeId}`
                }
            }
        }).promise();

        if (getItem.$response.data && getItem.Item) {
            console.info(
                "DynamoDBDelegate: ",
                "putOrUpdateItem: ",
                "getItem.Item: ",
                "if block: ",
                `item is ${JSON.stringify(getItem.Item)}`,
                `, data is ${JSON.stringify(getItem.$response.data)}`,
            );

            await this.updateItem({ tableName, db, attributeMap: getItem.Item, message });
        } else {
            console.info(
                "DynamoDBDelegate: ",
                "putOrUpdateItem: ",
                "getItem.Item: ",
                "else block: ",
                `item is ${JSON.stringify(getItem.Item)}`,
                `, data is ${JSON.stringify(getItem.$response.data)}`,
            );

            await this.putItem({ tableName, db, message });
        }

        return;
    }

    private updateItem({ tableName, db, attributeMap, message }: { tableName: string; db: DB; attributeMap: DB.AttributeMap, message: Message }): Promise<PromiseResult<DB.UpdateItemOutput, AWSError>> {
        console.log("DynamoDBDelegate: ", "updateItem called");

        const item: { [key: string]: any } = Converter.unmarshall(attributeMap);
        const data: [{ [key: string]: any }] = item.data;
        const nextIndex: number = data.length;

        return db.updateItem({
            TableName: tableName,
            Key: {
                "id": {
                    N: `${message.nodeId}`
                }
            },
            ExpressionAttributeNames: {
                "#d": "data"
            },
            ExpressionAttributeValues: {
                ":d": this.computeDataItem({ message })
            },
            UpdateExpression: `SET #d[${nextIndex}] = :d`
        }).promise();
    }

    private putItem({ tableName, db, message }: { tableName: string; db: DB; message: Message }): Promise<PromiseResult<DB.PutItemOutput, AWSError>> {
        console.log("DynamoDBDelegate: ", "putItem called");

        return db.putItem({
            TableName: tableName,
            Item: {
                "id": {
                    N: `${message.nodeId}`
                },
                "data": {
                    L: [
                        this.computeDataItem({ message })
                    ]
                }
            }
        }).promise();
    }

    private computeDataItem({ message }: { message: Message }): {
        M: {
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
        };
    } {
        console.log("DynamoDBDelegate: ", "computeDataItem called");

        return {
            M: {
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