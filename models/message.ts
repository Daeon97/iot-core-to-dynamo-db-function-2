export class Message {
    constructor(
        public nodeId: number,
        public latitude: number,
        public longitude: number,
        public batteryLevel: number,
    ) { }

    static fromObject(object: any): Message {
        return new Message(
            object.id,
            object.lat,
            object.lng,
            object.battery_level,
        );
    }
}