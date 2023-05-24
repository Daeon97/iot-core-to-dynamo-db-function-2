import { IoTHandler } from 'aws-lambda';
import { IoTCoreDelegate } from './delegates/iot-core-delegate';

export const lambdaHandler: IoTHandler = async (event) => {
    console.log("lambdaHandler triggered");

    const iotCoreDelegate: IoTCoreDelegate = new IoTCoreDelegate();
    await iotCoreDelegate.processMessages(event);
};