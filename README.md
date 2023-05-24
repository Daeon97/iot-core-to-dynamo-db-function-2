### IotCoreToDynamoDbFunction2
A simple AWS Lambda function that moves data from AWS IoT Core to AWS Dynamo DB.
This function uses a more flattened approach to store data in Dynamo DB, which
may be overkill if the nest is small. A parent, of this function
which can be found at https://github.com/Daeon97/iot-core-to-dynamo-db-function
uses a nested approach to store data in Dynamo DB